/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.rapid

import java.util.concurrent.atomic.AtomicInteger

import akka.cluster.Cluster
import akka.cluster.ClusterHeartbeatSender.Heartbeat
import akka.remote.{FailureDetector, FailureDetectorLoader}
import com.vrg.rapid.monitoring.IEdgeFailureDetectorFactory
import com.vrg.rapid.pb.{Endpoint, NodeStatus, ProbeMessage, RapidRequest, RapidResponse}
import org.slf4j.LoggerFactory
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.vrg.rapid.messaging.IMessagingClient

/**
 * Adapter for rapid's pluggable failure detector facility. We simply plugin the phi accrual FD.
 */
class RapidFailureDetectorFactory(address: Endpoint, cluster: Cluster, messagingClient: IMessagingClient) extends IEdgeFailureDetectorFactory {
  override def createInstance(subject: Endpoint, configurationId: Long, notifier: Runnable): Runnable = {
    val failureDetector: FailureDetector = FailureDetectorLoader.load(
      cluster.settings.FailureDetectorImplementationClass,
      cluster.settings.FailureDetectorConfig,
      cluster.system
    )
    new EdgeFailureDetector(address, subject, configurationId, notifier, failureDetector, cluster, messagingClient)
  }
}

object EdgeFailureDetector {
  val BootstrapCountThreshold = 30
}

class EdgeFailureDetector(address: Endpoint, subject: Endpoint, configurationId: Long, notifier: Runnable, failureDetector: FailureDetector, cluster: Cluster, messagingClient: IMessagingClient) extends Runnable {
  import EdgeFailureDetector._

  val log = LoggerFactory.getLogger(getClass)

  private var firstHeartbeatSent = false

  private var notified = false

  private val bootstrapResponseCount = new AtomicInteger(0)

  private val probeMessage = RapidRequest.newBuilder.setProbeMessage(
    ProbeMessage.newBuilder().setSender(address).setObserverConfigurationId(configurationId).build()
  ).build()

  private var sequenceNr = 0

  def selfHeartbeat(): Heartbeat = {
    sequenceNr += 1
    Heartbeat(cluster.selfAddress, sequenceNr, System.nanoTime())
  }

  override def run(): Unit = {

    if(!failureDetector.isAvailable && !notified) {
      fail()
    } else {
      if(!failureDetector.isMonitoring && !firstHeartbeatSent) {
        // this is the first ping, give the remote system a chance to start up by pinging it
        messagingClient.sendMessageBestEffort(subject, probeMessage)
        firstHeartbeatSent = true
      } else if(!failureDetector.isMonitoring && firstHeartbeatSent) {
        // wake up our failure detector
        failureDetector.heartbeat()
      } else {
        sendHeartbeat()
      }
    }
  }

  def sendHeartbeat(): Unit = {
    val response = messagingClient.sendMessageBestEffort(subject, probeMessage)
    Futures.addCallback(response, new ProbeCallback, cluster.system.dispatcher)
  }

  def fail(): Unit = {
    log.debug("Failing edge {}:{}", subject.getHostname.toStringUtf8, subject.getPort)
    notifier.run()
    notified = true
  }

  class ProbeCallback extends FutureCallback[RapidResponse] {
    override def onSuccess(result: RapidResponse): Unit = {
      val response = result.getProbeResponse
      if (response.getStatus == NodeStatus.BOOTSTRAPPING) {
        val bootstrapCount = bootstrapResponseCount.incrementAndGet()
        if (bootstrapCount <=  BootstrapCountThreshold) {
          failureDetector.heartbeat()
        } else if (!notified) {
          log.error(s"Endpoint {}:{} failed to bootstrap after {} ticks, marking it as faulty",
            subject.getHostname.toStringUtf8, subject.getPort, BootstrapCountThreshold)
          fail()
        }
      } else {
        failureDetector.heartbeat()
      }
    }
    override def onFailure(t: Throwable): Unit = {
      // no heartbeat update
    }
  }

}
