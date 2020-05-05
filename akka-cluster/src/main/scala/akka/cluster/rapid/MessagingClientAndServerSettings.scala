/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.rapid

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.vrg.rapid.pb.RapidRequest

import scala.concurrent.duration.FiniteDuration

final case class MessagingClientAndServerSettings(cluster: Cluster) {
  val DefaultTimeout = cluster.settings.Rapid.DefaultTimeout
  val DefaultRetries = cluster.settings.Rapid.DefaultRetries

  def messageTimeoutAndRetries(msg: RapidRequest): (FiniteDuration, Int) = {
    msg.getContentCase match {
      case RapidRequest.ContentCase.PROBEMESSAGE => (cluster.settings.HeartbeatExpectedResponseAfter, DefaultRetries)
      case RapidRequest.ContentCase.JOINMESSAGE => (DefaultTimeout * 50, 10)
      case RapidRequest.ContentCase.BROADCASTINGMESSAGE => (DefaultTimeout * 3, 10)
      case RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE => (DefaultTimeout * 3, 5)
      case _ => (DefaultTimeout, DefaultRetries)
    }
  }


}

object MessagingClientAndServerSettings {

  def apply(system: ActorSystem): MessagingClientAndServerSettings = MessagingClientAndServerSettings(Cluster(system))

}
