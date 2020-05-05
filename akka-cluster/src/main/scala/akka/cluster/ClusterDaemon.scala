/*
 * Copyright (C) 2009-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal
import akka.actor._
import akka.annotation.InternalApi
import akka.actor.SupervisorStrategy.Stop
import akka.cluster.MemberStatus._
import akka.cluster.ClusterEvent._
import akka.dispatch.{RequiresMessageQueue, UnboundedMessageQueueSemantics}
import akka.Done
import akka.pattern.ask
import akka.remote.{QuarantinedEvent => ClassicQuarantinedEvent}
import akka.remote.artery.QuarantinedEvent
import akka.util.Timeout
import com.github.ghik.silencer.silent
import com.google.common.net.HostAndPort
import com.google.common.primitives.Longs
import com.google.protobuf.ByteString
import com.typesafe.config.Config
import com.vrg.rapid.pb.Endpoint
import com.vrg.rapid.{NodeStatusChange, Settings, Cluster => RapidCluster}

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions


/**
 * Base trait for all cluster messages. All ClusterMessage's are serializable.
 */
trait ClusterMessage extends Serializable

/**
 * INTERNAL API
 * Cluster commands sent by the USER via
 * [[akka.cluster.Cluster]] extension
 * or JMX.
 */
@InternalApi
private[cluster] object ClusterUserAction {

  /**
   * Command to initiate join another node (represented by `address`).
   * Join will be sent to the other node.
   */
  @SerialVersionUID(1L)
  final case class JoinTo(address: Address)

  /**
   * Command to leave the cluster.
   */
  @SerialVersionUID(1L)
  final case class Leave(address: Address) extends ClusterMessage

  /**
   * Command to mark node as temporary down.
   */
  @SerialVersionUID(1L)
  final case class Down(address: Address) extends ClusterMessage

}

/**
 * INTERNAL API
 */
@InternalApi
private[cluster] object InternalClusterAction {

  /**
   * Command to join the cluster. Sent when a node wants to join another node (the receiver).
   *
   * @param node the node that wants to join the cluster
   */
  @SerialVersionUID(1L)
  final case class Join(node: UniqueAddress, roles: Set[String]) extends ClusterMessage

  /**
   * Reply to Join
   *
   * @param from the sender node in the cluster, i.e. the node that received the Join command
   */
  @SerialVersionUID(1L)
  final case class Welcome(from: UniqueAddress, gossip: Gossip) extends ClusterMessage

  /**
   * Command to initiate the process to join the specified
   * seed nodes.
   */
  final case class JoinSeedNodes(seedNodes: immutable.IndexedSeq[Address])

  /**
   * Start message of the process to join one of the seed nodes.
   * The node sends `InitJoin` to all seed nodes, which replies
   * with `InitJoinAck`. The first reply is used others are discarded.
   * The node sends `Join` command to the seed node that replied first.
   * If a node is uninitialized it will reply to `InitJoin` with
   * `InitJoinNack`.
   */
  case object JoinSeedNode extends DeadLetterSuppression

  sealed trait ConfigCheck
  case object UncheckedConfig extends ConfigCheck
  case object IncompatibleConfig extends ConfigCheck

  /**
   * Node with version 2.5.9 or earlier is joining. The serialized
   * representation of `InitJoinAck` must be a plain `Address` for
   * such a joining node.
   */
  case object ConfigCheckUnsupportedByJoiningNode extends ConfigCheck

  final case class CompatibleConfig(clusterConfig: Config) extends ConfigCheck

  /**
   * see JoinSeedNode
   */
  @SerialVersionUID(1L)
  case class InitJoin(configOfJoiningNode: Config) extends ClusterMessage with DeadLetterSuppression

  /**
   * see JoinSeedNode
   */
  @SerialVersionUID(1L)
  final case class InitJoinAck(address: Address, configCheck: ConfigCheck)
      extends ClusterMessage
      with DeadLetterSuppression

  /**
   * see JoinSeedNode
   */
  @SerialVersionUID(1L)
  final case class InitJoinNack(address: Address) extends ClusterMessage with DeadLetterSuppression

  final case class ExitingConfirmed(node: UniqueAddress) extends ClusterMessage with DeadLetterSuppression

  /**
   * Marker interface for periodic tick messages
   */
  sealed trait Tick

  case object GossipTick extends Tick

  case object GossipSpeedupTick extends Tick

  case object ReapUnreachableTick extends Tick

  case object LeaderActionsTick extends Tick

  case object PublishStatsTick extends Tick

  final case class SendGossipTo(address: Address)

  case object GetClusterCoreRef

  /**
   * Command to [[akka.cluster.ClusterDaemon]] to create a
   * [[akka.cluster.OnMemberStatusChangedListener]].
   */
  final case class AddOnMemberUpListener(callback: Runnable) extends NoSerializationVerificationNeeded

  final case class AddOnMemberRemovedListener(callback: Runnable) extends NoSerializationVerificationNeeded

  sealed trait SubscriptionMessage
  final case class Subscribe(subscriber: ActorRef, initialStateMode: SubscriptionInitialStateMode, to: Set[Class[_]])
      extends SubscriptionMessage
  final case class Unsubscribe(subscriber: ActorRef, to: Option[Class[_]])
      extends SubscriptionMessage
      with DeadLetterSuppression

  /**
   * @param receiver [[akka.cluster.ClusterEvent.CurrentClusterState]] will be sent to the `receiver`
   */
  final case class SendCurrentClusterState(receiver: ActorRef) extends SubscriptionMessage

  sealed trait PublishMessage
  final case class PublishChanges(state: MembershipState) extends PublishMessage
  final case class PublishEvent(event: ClusterDomainEvent) extends PublishMessage

  final case object ExitingCompleted

}

/**
 * INTERNAL API.
 *
 * Supervisor managing the different Cluster daemons.
 */
@InternalApi
private[cluster] final class ClusterDaemon
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
  import InternalClusterAction._
  // Important - don't use Cluster(context.system) in constructor because that would
  // cause deadlock. The Cluster extension is currently being created and is waiting
  // for response from GetClusterCoreRef in its constructor.
  // Child actors are therefore created when GetClusterCoreRef is received
  var coreSupervisor: Option[ActorRef] = None

  val clusterShutdown = Promise[Done]()
  val coordShutdown = CoordinatedShutdown(context.system)
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterLeave, "leave") {
    val sys = context.system
    () =>
      if (Cluster(sys).isTerminated || Cluster(sys).selfMember.status == Down)
        Future.successful(Done)
      else {
        implicit val timeout = Timeout(coordShutdown.timeout(CoordinatedShutdown.PhaseClusterLeave))
        self.ask(CoordinatedShutdownLeave.LeaveReq).mapTo[Done]
      }
  }
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterShutdown, "wait-shutdown") { () =>
    clusterShutdown.future
  }

  override def postStop(): Unit = {
    clusterShutdown.trySuccess(Done)
    if (Cluster(context.system).settings.RunCoordinatedShutdownWhenDown) {
      // if it was stopped due to leaving CoordinatedShutdown was started earlier
      coordShutdown.run(CoordinatedShutdown.ClusterDowningReason)
    }
  }

  def createChildren(): Unit = {
    coreSupervisor = Some(
      context.actorOf(
        Props(classOf[ClusterCoreSupervisor]).withDispatcher(context.props.dispatcher),
        name = "core"))
    context.actorOf(
      ClusterHeartbeatReceiver.props(() => Cluster(context.system)).withDispatcher(context.props.dispatcher),
      name = "heartbeatReceiver")
  }

  def receive = {
    case msg: GetClusterCoreRef.type =>
      if (coreSupervisor.isEmpty)
        createChildren()
      coreSupervisor.foreach(_.forward(msg))
    case AddOnMemberUpListener(code) =>
      context.actorOf(Props(classOf[OnMemberStatusChangedListener], code, Up).withDeploy(Deploy.local))
    case AddOnMemberRemovedListener(code) =>
      context.actorOf(Props(classOf[OnMemberStatusChangedListener], code, Removed).withDeploy(Deploy.local))
    case CoordinatedShutdownLeave.LeaveReq =>
      val ref = context.actorOf(CoordinatedShutdownLeave.props().withDispatcher(context.props.dispatcher))
      // forward the ask request
      ref.forward(CoordinatedShutdownLeave.LeaveReq)
  }

}

/**
 * INTERNAL API.
 *
 * ClusterCoreDaemon and ClusterDomainEventPublisher can't be restarted because the state
 * would be obsolete. Shutdown the member if any those actors crashed.
 */
@InternalApi
private[cluster] final class ClusterCoreSupervisor
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  // Important - don't use Cluster(context.system) in constructor because that would
  // cause deadlock. The Cluster extension is currently being created and is waiting
  // for response from GetClusterCoreRef in its constructor.
  // Child actors are therefore created when GetClusterCoreRef is received

  var coreDaemon: Option[ActorRef] = None

  def createChildren(): Unit = {
    val publisher =
      context.actorOf(Props[ClusterDomainEventPublisher].withDispatcher(context.props.dispatcher), name = "publisher")
    coreDaemon = Some(
      context.watch(context.actorOf(
        Props(classOf[ClusterCoreDaemon], publisher).withDispatcher(context.props.dispatcher),
        name = "daemon")))
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case NonFatal(e) =>
        Cluster(context.system).ClusterLogger.logError(e, "crashed, [{}] - shutting down...", e.getMessage)
        self ! PoisonPill
        Stop
    }

  override def postStop(): Unit = Cluster(context.system).shutdown()

  def receive = {
    case InternalClusterAction.GetClusterCoreRef =>
      if (coreDaemon.isEmpty)
        createChildren()
      coreDaemon.foreach(sender() ! _)
  }
}

/**
 * INTERNAL API.
 */
@InternalApi
private[cluster] object ClusterCoreDaemon {
  val NumberOfGossipsBeforeShutdownWhenLeaderExits = 5
  val MaxGossipsBeforeShuttingDownMyself = 5
  val MaxTicksBeforeShuttingDownMyself = 4

}

/**
 * INTERNAL API.
 */
@InternalApi
private[cluster] class ClusterCoreDaemon(publisher: ActorRef)
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
  import InternalClusterAction._

  val cluster = Cluster(context.system)
  import cluster.ClusterLogger._
  import cluster.selfAddress
  import cluster.settings._

  val selfDc = cluster.selfDataCenter

  protected def selfUniqueAddress = cluster.selfUniqueAddress

  var seedNodes = SeedNodes

  // note that self is not initially member,
  // and the Gossip is not versioned for this 'Node' yet
  var membershipState = MembershipState(
    Gossip.empty,
    cluster.selfUniqueAddress,
    cluster.settings.SelfDataCenter,
    cluster.settings.MultiDataCenter.CrossDcConnections)

  var rapidCluster: Option[RapidCluster] = None

  var exitingTasksInProgress = false
  val selfExiting = Promise[Done]()
  val coordShutdown = CoordinatedShutdown(context.system)
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterExiting, "wait-exiting") { () =>
    if (membershipState.latestGossip.members.isEmpty)
      Future.successful(Done) // not joined yet
    else
      selfExiting.future
  }
  coordShutdown.addTask(CoordinatedShutdown.PhaseClusterExitingDone, "exiting-completed") {
    val sys = context.system
    () =>
      if (Cluster(sys).isTerminated || Cluster(sys).selfMember.status == Down)
        Future.successful(Done)
      else {
        implicit val timeout = Timeout(coordShutdown.timeout(CoordinatedShutdown.PhaseClusterExitingDone))
        self.ask(ExitingCompleted).mapTo[Done]
      }
  }
  var exitingConfirmed = Set.empty[UniqueAddress]

  override def preStart(): Unit = {
    subscribeQuarantinedEvent()

    if (seedNodes.isEmpty) {
      if (isClusterBootstrapUsed)
        logDebug("Cluster Bootstrap is used for joining")
      else
        logInfo(
          "No seed-nodes configured, manual cluster join required, see " +
          "https://doc.akka.io/docs/akka/current/typed/cluster.html#joining")
    } else {
      self ! JoinSeedNodes(seedNodes)
    }
  }

  @silent("deprecated")
  private def subscribeQuarantinedEvent(): Unit = {
    context.system.eventStream.subscribe(self, classOf[QuarantinedEvent])
    context.system.eventStream.subscribe(self, classOf[ClassicQuarantinedEvent])
  }

  private def isClusterBootstrapUsed: Boolean = {
    val conf = context.system.settings.config
    conf.hasPath("akka.management.cluster.bootstrap") &&
    conf.hasPath("akka.management.http.route-providers") &&
    conf
      .getStringList("akka.management.http.route-providers")
      .contains("akka.management.cluster.bootstrap.ClusterBootstrap$")
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
    selfExiting.trySuccess(Done)
  }

  def uninitialized: Actor.Receive =
    ({
      case InitJoin(_) =>
        logInfo("Received InitJoin message from [{}], but this node is not initialized yet", sender())
        sender() ! InitJoinNack(selfAddress)
      case ClusterUserAction.JoinTo(address) =>
        join(address)
      case JoinSeedNodes(newSeedNodes) =>
        if (newSeedNodes.isEmpty) {
          logError("No seed nodes, cannot do anything")
        } else {
          context.become(tryingToJoin())
          self ! ClusterUserAction.JoinTo(newSeedNodes.head)
        }
      case msg: SubscriptionMessage =>
        publisher.forward(msg)
    }: Actor.Receive).orElse(receiveExitingCompleted)

  def tryingToJoin(): Actor.Receive =
    ({
      case InitJoin(_) =>
        logInfo("Received InitJoin message from [{}], but this node is not a member yet", sender())
        sender() ! InitJoinNack(selfAddress)
      case ClusterUserAction.JoinTo(address) =>
        becomeUninitialized()
        join(address)
      case msg: SubscriptionMessage => publisher.forward(msg)
    }: Actor.Receive).orElse(receiveExitingCompleted)


  def becomeUninitialized(): Unit = {
    context.become(uninitialized)
  }

  def becomeInitialized(): Unit = {
    val externalHeartbeatProps = Props(new CrossDcHeartbeatSender()).withDispatcher(UseDispatcher)
    context.actorOf(externalHeartbeatProps, name = "crossDcHeartbeatSender")

    context.become(initialized)
  }

  def initialized: Actor.Receive =
    ({
      case ClusterUserAction.Leave(_)            => leaving()
      case msg: SubscriptionMessage              => publisher.forward(msg)
      case QuarantinedEvent(ua)                  => quarantined(UniqueAddress(ua))
      case ClassicQuarantinedEvent(address, uid) => quarantined(UniqueAddress(address, uid))
      case ClusterUserAction.JoinTo(address) =>
        logInfo("Trying to join [{}] when already part of a cluster, ignoring", address)
      case JoinSeedNodes(nodes) =>
        logInfo("Trying to join seed nodes [{}] when already part of a cluster, ignoring", nodes.mkString(", "))
      case ExitingConfirmed(address) => receiveExitingConfirmed(address)
    }: Actor.Receive).orElse(receiveExitingCompleted)

  def receiveExitingCompleted: Actor.Receive = {
    case ExitingCompleted =>
      exitingCompleted()
      sender() ! Done // reply to ask
  }

  def receive = uninitialized

  override def unhandled(message: Any): Unit = message match {
    case _: Tick             =>
    case _: GossipEnvelope   =>
    case _: GossipStatus     =>
    case _: ExitingConfirmed =>
    case other               => super.unhandled(other)
  }


  /**
   * Try to join this cluster node with the node specified by `address`.
   * It's only allowed to join from an empty state, i.e. when not already a member.
   * A `Join(selfUniqueAddress)` command is sent to the node to join,
   * which will reply with a `Welcome` message.
   */
  def join(address: Address): Unit = {
    if (address.protocol != selfAddress.protocol)
      logWarning(
        "Trying to join member with wrong protocol, but was ignored, expected [{}] but was [{}]",
        selfAddress.protocol,
        address.protocol)
    else if (address.system != selfAddress.system)
      logWarning(
        "Trying to join member with wrong ActorSystem name, but was ignored, expected [{}] but was [{}]",
        selfAddress.system,
        address.system)
    else {
      val selfHostPort = HostAndPort.fromParts(selfAddress.host.get, selfAddress.port.get)
      val selfEndpoint = Endpoint.newBuilder().setHostname(ByteString.copyFromUtf8(selfAddress.host.get)).setPort(selfAddress.port.get).build()
      val akkaMessaging = new rapid.AkkaRemoteMessagingClientAndServer(cluster, context)
      val failureDetectorFactory = new rapid.RapidFailureDetectorFactory(selfEndpoint, cluster, akkaMessaging)

      def createRapidClusterBuilder = {

        val settings = new Settings()
        settings.setFailureDetectorIntervalInMs(cluster.settings.HeartbeatInterval.toMillis.toInt)
        settings.setBatchingWindowInMs(cluster.settings.Rapid.BatchingWindow.toMillis.toInt)
        settings.setConsensusBatchingWindowInMs(cluster.settings.Rapid.ConsensusBatchingWindow.toMillis.toInt)
        settings.setConsensusFallbackTimeoutBaseDelayInMs(cluster.settings.Rapid.ConsensusFallbackTimeoutBaseDelay.toMillis.toInt)
        settings.setMembershipViewUpdateTimeoutInMs(cluster.settings.Rapid.MembershipViewUpdateTimeout.toMillis.toInt)

        val uid: Array[Byte] = Longs.toByteArray(selfUniqueAddress.longUid)
        new RapidCluster.Builder(selfHostPort)
          .useSettings(settings)
          .setMetadata(Map(
            "uid" -> ByteString.copyFrom(uid),
            "roles" -> ByteString.copyFromUtf8(cluster.selfRoles.mkString(","))
          ).asJava)
          .setMessagingClientAndServer(akkaMessaging, akkaMessaging)
          .setEdgeFailureDetectorFactory(failureDetectorFactory)
          .addSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE, (_, viewChanges) => {
              onViewChange(viewChanges.asScala.toList)
            }
          )
          .withConsistentHashBroadcasting(cluster.settings.Rapid.UseConsistentHashBroadcasting, cluster.settings.Rapid.ActAsConsistentHashBroadcaster)
          .addSubscription(com.vrg.rapid.ClusterEvents.KICKED, (_, _) => onKicked())
      }

      if (address == selfAddress) {
        val clusterBuilder = createRapidClusterBuilder
        val rapidCluster = clusterBuilder.start()
        onMembershipInitialized(rapidCluster)
      } else {

        import scala.concurrent.duration._
        val Attempts = 3
        val Delay = 5.seconds

        def joinAttempt(attemptNumber: Int): Unit = {
          logInfo("Attempting to join cluster {}/{}", attemptNumber, Attempts)
          try {
            val clusterBuilder = createRapidClusterBuilder
            val seedNodeAddress = HostAndPort.fromParts(address.getHost().get(), address.port.get)
            val rapidCluster = clusterBuilder.join(seedNodeAddress)
            if (rapidCluster.getMemberlist.isEmpty) {
              throw new IllegalStateException("Joining an empty cluster?!")
            }
            onMembershipInitialized(rapidCluster)
          } catch {
            case NonFatal(t) =>
              logError(t, "Join attempt {}/{} failed", attemptNumber, Attempts)
              if (attemptNumber < Attempts) {
                Thread.sleep(Delay.toMillis)
                joinAttempt(attemptNumber + 1)
              } else {
                logError("Maximum join attempts reached, giving up")
                throw t
              }

          }
        }

        joinAttempt(1)

      }
    }
  }

  private def onViewChange(viewChanges: List[NodeStatusChange]): Unit = {
    logInfo("Processing view change of size {}", viewChanges.size)
    val changedMembers = viewChanges.flatMap { nodeStatusChange =>
      val metadata = nodeStatusChange.getMetadata.getMetadataMap
      try {
        val uidMetadata = metadata.get("uid")
        val uid = Longs.fromByteArray(uidMetadata.toByteArray)
        val rolesMetadata = metadata.get("roles")
        val roles = rolesMetadata.toStringUtf8.split(",").toSet
        val status = if(nodeStatusChange.getStatus.getNumber == 0) Up else Removed
        Some(Member(UniqueAddress(nodeStatusChange.getEndpoint, uid), roles).copy(status = status))
      } catch {
        case NonFatal(t) =>
          logError(t, "Metadata not as expected? Endpoint {} metadata {}", nodeStatusChange.getEndpoint, nodeStatusChange.getMetadata)
          None
      }
    }.toSet

    // rapid is purely a membership protocol, either a member is added or it is removed
    // we don't try to emulate Akka's intermediary membership states here (reconfiguration),
    // we just feed it with the output of rapid. turns out this works quite well
    val oldMembers = membershipState.members
    val addedMembers = assignUpNumber(changedMembers.diff(oldMembers), oldMembers)
    val removedMembers = changedMembers.filter(_.status == Removed)
    val updatedMembers = (oldMembers ++ addedMembers) -- removedMembers

    if (updatedMembers.isEmpty && oldMembers.nonEmpty) {
      // this transition should not happen, but in case it does, leave gracefully
      logWarning("Nobody else is left in the cluster, leaving as well")
      leaving()
    } else {
      val gossip = Gossip(updatedMembers)
      membershipState = membershipState.copy(latestGossip = gossip)
      publishMembershipState()
    }



  }

  private def onKicked(): Unit = {
    // this node has been kicked out by rapid, which translates to being downed
    logWarning("Node has been marked as DOWN. Shutting down myself")
    rapidCluster.foreach(_.shutdown())
    updateSelfState(MemberStatus.Down)
    publishMembershipState()
    shutdown()
  }

  private def assignUpNumber(newMembers: Set[Member], existingMembers: Set[Member]): Set[Member] = {
      def isJoiningToUp(m: Member): Boolean = m.status == Up

    if (newMembers.nonEmpty && existingMembers.nonEmpty) {
      newMembers.collect {
        var upNumber = 0

        {
          case m if m.dataCenter == selfDc && isJoiningToUp(m) =>
            // Move JOINING => UP (once all nodes have seen that this node is JOINING, i.e. we have a convergence)
            // and minimum number of nodes have joined the cluster
            if (upNumber == 0) {
              // It is alright to use same upNumber as already used by a removed member, since the upNumber
              // is only used for comparing age of current cluster members (Member.isOlderThan)
              val youngest = existingMembers.maxBy(m => if (m.upNumber == Int.MaxValue) 0 else m.upNumber)
              upNumber = 1 + (if (youngest.upNumber == Int.MaxValue) 0 else youngest.upNumber)
            } else {
              upNumber += 1
            }
            m.copyUp(upNumber)
        }
      }
    } else newMembers
  }

  private def updateSelfState(status: MemberStatus): Unit = {
    val updatedMembers = membershipState.latestGossip.members.map {
      case member if member.uniqueAddress == cluster.selfUniqueAddress => member.copy(status = status)
      case member => member
    }
    membershipState = membershipState.copy(latestGossip = membershipState.latestGossip.copy(updatedMembers))

  }

  private def onMembershipInitialized(cluster: RapidCluster): Unit = {
    this.rapidCluster = Some(cluster)
    becomeInitialized()
  }

  private implicit def addressFromEndpoint(endpoint: Endpoint): Address =
    Address(selfAddress.protocol, selfAddress.system, endpoint.getHostname.toStringUtf8, endpoint.getPort)

  /**
   * State transition to LEAVING.
   */
  def leaving(): Unit = {
    updateSelfState(Leaving)
    publishMembershipState()
    rapidCluster.foreach { cluster =>
      cluster.shutdown()
    }
    updateSelfState(Exiting)
    publishMembershipState()
  }

  def quarantined(node: UniqueAddress): Unit = {
    val localGossip = membershipState.latestGossip
    if (localGossip.hasMember(node)) {
      val newReachability = membershipState.latestGossip.overview.reachability.terminated(selfUniqueAddress, node)
      val newOverview = localGossip.overview.copy(reachability = newReachability)
      val newGossip = localGossip.copy(overview = newOverview)
      membershipState = membershipState.copy(latestGossip = newGossip)
      logWarning(
        ClusterLogMarker.unreachable(node.address),
        "Marking node as TERMINATED [{}], due to quarantine. Node roles [{}]. " +
          "It must still be marked as down before it's removed.",
        node.address,
        cluster.selfRoles.mkString(","))
      publishMembershipState()
    }
  }

  def exitingCompleted() = {
    shutdown()
  }

  def receiveExitingConfirmed(node: UniqueAddress): Unit = {
    logInfo("Exiting confirmed [{}]", node.address)
    exitingConfirmed += node
  }


  /**
   * This method is called when a member sees itself as Exiting or Down.
   */
  def shutdown(): Unit = cluster.shutdown()

  def publishMembershipState(): Unit = {
    publisher ! PublishChanges(membershipState)

  }

}

/**
 * INTERNAL API
 *
 * The supplied callback will be run, once, when current cluster member come up with the same status.
 */
@InternalApi
private[cluster] class OnMemberStatusChangedListener(callback: Runnable, status: MemberStatus) extends Actor {
  import ClusterEvent._
  private val cluster = Cluster(context.system)
  import cluster.ClusterLogger._

  private val to = status match {
    case Up      => classOf[MemberUp]
    case Removed => classOf[MemberRemoved]
    case other =>
      throw new IllegalArgumentException(s"Expected Up or Removed in OnMemberStatusChangedListener, got [$other]")
  }

  override def preStart(): Unit =
    cluster.subscribe(self, to)

  override def postStop(): Unit = {
    if (status == Removed)
      done()
    cluster.unsubscribe(self)
  }

  def receive = {
    case state: CurrentClusterState =>
      if (state.members.exists(isTriggered))
        done()
    case MemberUp(member) =>
      if (isTriggered(member))
        done()
    case MemberRemoved(member, _) =>
      if (isTriggered(member))
        done()
  }

  private def done(): Unit = {
    try callback.run()
    catch {
      case NonFatal(e) => logError(e, "[{}] callback failed with [{}]", s"On${to.getSimpleName}", e.getMessage)
    } finally {
      context.stop(self)
    }
  }

  private def isTriggered(m: Member): Boolean =
    m.uniqueAddress == cluster.selfUniqueAddress && m.status == status

}

/**
 * INTERNAL API
 */
@InternalApi
@SerialVersionUID(1L)
private[cluster] final case class GossipStats(
    receivedGossipCount: Long = 0L,
    mergeCount: Long = 0L,
    sameCount: Long = 0L,
    newerCount: Long = 0L,
    olderCount: Long = 0L) {

  def incrementMergeCount(): GossipStats =
    copy(mergeCount = mergeCount + 1, receivedGossipCount = receivedGossipCount + 1)

  def incrementSameCount(): GossipStats =
    copy(sameCount = sameCount + 1, receivedGossipCount = receivedGossipCount + 1)

  def incrementNewerCount(): GossipStats =
    copy(newerCount = newerCount + 1, receivedGossipCount = receivedGossipCount + 1)

  def incrementOlderCount(): GossipStats =
    copy(olderCount = olderCount + 1, receivedGossipCount = receivedGossipCount + 1)

  def :+(that: GossipStats): GossipStats = {
    GossipStats(
      this.receivedGossipCount + that.receivedGossipCount,
      this.mergeCount + that.mergeCount,
      this.sameCount + that.sameCount,
      this.newerCount + that.newerCount,
      this.olderCount + that.olderCount)
  }

  def :-(that: GossipStats): GossipStats = {
    GossipStats(
      this.receivedGossipCount - that.receivedGossipCount,
      this.mergeCount - that.mergeCount,
      this.sameCount - that.sameCount,
      this.newerCount - that.newerCount,
      this.olderCount - that.olderCount)
  }

}

/**
 * INTERNAL API
 */
@InternalApi
@SerialVersionUID(1L)
private[cluster] final case class VectorClockStats(versionSize: Int = 0, seenLatest: Int = 0)
