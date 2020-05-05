/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.rapid

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, ActorSelection, Address, Props, RootActorPath, Stash, Status, Timers}
import akka.cluster.Cluster
import akka.cluster.rapid.RapidMembershipService.ReportTick
import com.google.common.util.concurrent.{ListenableFuture, SettableFuture}
import com.vrg.rapid.MembershipService
import com.vrg.rapid.messaging.{IMessagingClient, IMessagingServer}
import com.vrg.rapid.pb.{Endpoint, NodeStatus, ProbeResponse, RapidRequest, RapidResponse}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.pattern.pipe
import akka.remote.RemoteSettings
import org.HdrHistogram.Histogram
/**
 * Implementation of rapid messaging using Akka's remoting
 */
class AkkaRemoteMessagingClientAndServer(override val cluster: Cluster, val context: ActorContext) extends IMessagingClient with IMessagingServer with EndpointAware {
  import AkkaRemoteMessagingServer._

  val settings = MessagingClientAndServerSettings(context.system)

  import context.dispatcher

  val log = LoggerFactory.getLogger(getClass)

  private val remoteSettings = new RemoteSettings(cluster.system.settings.config)

  var membershipService: Option[MembershipService] = None

  var messagingServer: Option[ActorRef] = None

  override def sendMessage(endpoint: Endpoint, rapidRequest: RapidRequest): ListenableFuture[RapidResponse] = {
    sendMessage(endpoint, rapidRequest, settings.DefaultRetries)
  }

  override def sendMessageBestEffort(endpoint: Endpoint, rapidRequest: RapidRequest): ListenableFuture[RapidResponse] = {
    sendMessage(endpoint, rapidRequest, 0)
  }

  private def sendMessage(endpoint: Endpoint, rapidRequest: RapidRequest, retries: Int): ListenableFuture[RapidResponse] = {
    import akka.pattern.ask
    import akka.pattern.retry

    val (configuredTimeout, configuredRetries) = settings.messageTimeoutAndRetries(rapidRequest)

    // when creating a lot of outbound connections at the same time, we'll experience delays
    // would be nicer to know whether an association already exists
    val messageTimeout = if (cluster.settings.Rapid.ActAsConsistentHashBroadcaster) {
      configuredTimeout + remoteSettings.Artery.Advanced.Tcp.ConnectionTimeout
    } else {
      configuredTimeout
    }

    val messageRetries = Math.max(retries, configuredRetries)

    def makeRequest(): Future[RapidResponse] = {
      if (!IgnoredRequestTypes(rapidRequest.getContentCase)) {
        log.debug("Sending request of type {} and size {} to {}:{}",
          rapidRequest.getContentCase.name(), rapidRequest.getSerializedSize, endpoint.getHostname.toStringUtf8, endpoint.getPort)
      }

      if (addressFromEndpoint(endpoint) == cluster.selfAddress) {
        // use the direct path, don't attempt a selection to ourselves
        messagingServer.map { ref =>
          ref.ask(rapidRequest)(messageTimeout).mapTo[RapidResponse]
        } getOrElse {
          Future.failed(new IllegalStateException("Server not ready"))
        }
      } else {
        clusterCoreMessaging(endpoint).ask(rapidRequest)(messageTimeout).mapTo[RapidResponse]
      }
    }

    val f =
      retry(
        () => makeRequest(),
        messageRetries,
        0.seconds
      )(context.dispatcher, context.system.scheduler)
    val settableFuture = SettableFuture.create[RapidResponse]()
    f.onComplete {
      case Success(response: RapidResponse) =>
        if (!IgnoredResponseTypes(response.getContentCase)) {
          log.debug("Received response {}", response.getContentCase.name())

        }
        settableFuture.set(response)
      case Success(response) =>
        log.error("Received unexpected response from rapid: {}", response)
        settableFuture.setException(new IllegalStateException("Unexpected response " + response))
      case Failure(exception) =>
        log.warn("""{"metric": "FAILED_MESSAGE", "requestType": "{}", "sender": "{}:{}", "recipient": "{}:{}", "size": {}, "timeout": {}}""",
          rapidRequest.getContentCase.name(), cluster.selfAddress.host.get, cluster.selfAddress.port.get,
          endpoint.getHostname.toStringUtf8, endpoint.getPort, rapidRequest.getSerializedSize, messageTimeout.toMillis)
        settableFuture.setException(exception)
    }
    settableFuture
  }

  override def start(): Unit = {
    // start child actors that send and receive the messages
    // the rapid implementation might end up not calling shutdown() in all cases so we might have this one hanging around already
    messagingServer = context.child("messaging")
    if (messagingServer.isEmpty) {
          messagingServer = Some(context.actorOf(Props(new AkkaRemoteMessagingServer()), "messaging"))
        }
        membershipService.foreach { membership =>
          messagingServer.foreach { messaging =>
            messaging ! membership
          }
        }
      }

      override def shutdown(): Unit = {
      messagingServer.foreach { s =>
        context.stop(s)
      }
      messagingServer = None
      membershipService = None
    }

    override def setMembershipService(membershipService: MembershipService): Unit = {
      this.membershipService = Some(membershipService)
      // non-seed nodes only get their service once they have joined
      messagingServer.foreach { s =>
        s ! membershipService
      }
    }

  }

  class AkkaRemoteMessagingServer extends Actor with ActorLogging with EndpointAware {
    var membershipService: ActorRef = ActorRef.noSender

    val DefaultProbeResponse = RapidResponse.newBuilder().setProbeResponse(ProbeResponse.newBuilder().setStatus(NodeStatus.OK).build()).build()
    val BootstrappingProbeResponse = RapidResponse.newBuilder().setProbeResponse(ProbeResponse.newBuilder().setStatus(NodeStatus.BOOTSTRAPPING).build()).build()

    override def receive: Receive = {
      case msg: RapidRequest if msg != null && membershipService != ActorRef.noSender =>
        if (msg.getContentCase == RapidRequest.ContentCase.PROBEMESSAGE) {
          // don't bother going to the rapid implementation which might be busy doing something else
          sender() ! DefaultProbeResponse
        } else {
          membershipService forward msg
      }
    case msg: RapidRequest if msg != null && membershipService == ActorRef.noSender =>
      if (msg.getContentCase == RapidRequest.ContentCase.PROBEMESSAGE) {
        // we aren't ready yet, but that's okay
        sender() ! BootstrappingProbeResponse
      } else {
        log.debug("Received message but membership service not available")
      }
    case membership: MembershipService =>
      log.debug("Setting membership service")
      // if we're not seed node, we don't have a membership service to start with
      // once we've joined, we are initialized
      membershipService = context.actorOf(Props(new RapidMembershipService(membership)), "membership")
    case msg if membershipService == ActorRef.noSender =>
        log.debug("Received message but no membership service set, msg: {}", msg)
      // we're not yet ready to deal with this message
      // drop it, rapid is designed this way
    case msg if msg == null => // ignore
  }

}

class RapidMembershipService(membershipService: MembershipService) extends Actor with ActorLogging with Stash with Timers {
  import AkkaRemoteMessagingServer._

  import context.dispatcher

  val settings = MessagingClientAndServerSettings(context.system)

  val histogram = new Histogram(30.minutes.toMillis, 3)
  var msgCount = 0

  timers.startTimerWithFixedDelay(ReportTick, ReportTick, 10.seconds)

  def receive: Receive = ready

  def ready: Receive = {
    case request: RapidRequest =>
      processRequest(request)
    case result: RapidResult =>
      processResult(result)
    case Status.Failure(t) =>
      log.error("Failed to process request", t)
    case ReportTick =>
      report()
  }

  private def processRequest(request: RapidRequest): Unit = {
    msgCount += 1
    val originalSender = sender()
    if (!IgnoredRequestTypes(request.getContentCase)) {
      log.debug("Received request {} of size {} from {}", request.getContentCase.name(), request.getSerializedSize, sender().path.toSerializationFormat)
    }

    val (timeout, _) = settings.messageTimeoutAndRetries(request)

    val result: Future[RapidResponse] = toScalaFuture(membershipService.handleMessage(request), context.dispatcher)
    val start = System.nanoTime()
    result
      .map { r =>
        val durationMillis = FiniteDuration(System.nanoTime() - start, TimeUnit.NANOSECONDS).toMillis
        histogram.recordValue(durationMillis)
        // just to get a sense of whether we're running into trouble because of the implementation performance
        if(durationMillis >= (timeout / 2).toMillis) {
          log.warning("""{"metric": "SLOW_PROCESSING", "requestType": "{}", "duration": {}, "sender": "{}", "size": {}}""",
            request.getContentCase.name(), durationMillis, originalSender.path.address, request.getSerializedSize)
        }
        RapidResult(r, originalSender)
      } pipeTo self
  }

  private def processResult(result: RapidResult): Unit = {
    if (result.response != null) {
      if (!IgnoredResponseTypes(result.response.getContentCase)) {
        log.debug("Result of type {} computed", result.response.getContentCase.name())
      }
      result.sender ! result.response
    } else {
      // send something back in order to complete the promise
      result.sender ! RapidResponse.getDefaultInstance
    }
  }

  private def report(): Unit = {
    if (msgCount > 0) {
      log.info("""{"msgCount": {}, "pp50": {}, "pp99": {}, "pp9999": {}}""", msgCount,
        histogram.getValueAtPercentile(50), histogram.getValueAtPercentile(99), histogram.getValueAtPercentile(99.99))
    }
  }
}

object RapidMembershipService {
  case object ReportTick
}

object AkkaRemoteMessagingServer {
  final case class RapidResult(response: RapidResponse, sender: ActorRef)

  val IgnoredRequestTypes = Set(RapidRequest.ContentCase.PROBEMESSAGE)
  val IgnoredResponseTypes = Set(RapidResponse.ContentCase.CONSENSUSRESPONSE, RapidResponse.ContentCase.PROBERESPONSE, RapidResponse.ContentCase.PROBERESPONSE, RapidResponse.ContentCase.CONTENT_NOT_SET)
}

trait EndpointAware { self =>
  val context: ActorContext

  val cluster = Cluster(context.system)


  protected def clusterCoreMessaging(address: Address): ActorSelection =
    context.actorSelection(RootActorPath(address) / "system" / "cluster" / "core" / "daemon" / "messaging")

  protected implicit def addressFromEndpoint(endpoint: Endpoint): Address =
    Address(cluster.selfAddress.protocol, cluster.selfAddress.system, endpoint.getHostname.toStringUtf8, endpoint.getPort)

}