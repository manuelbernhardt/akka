/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.rapid

import com.google.protobuf.ByteString
import com.vrg.rapid.pb.{Endpoint, JoinResponse, JoinStatusCode, Metadata, NodeId}

import scala.jdk.CollectionConverters._
import scala.util.Random

// Utility method to figure out how large the largest Rapid message in a 10k cluster will be
object MessageSizes extends App  {

  val endpoints = for {
    i <- 0 until 40
    j <- 0 until 250
  } yield {
    Endpoint
      .newBuilder()
      .setHostname(ByteString.copyFromUtf8(s"172.31.$i.$j"))
      //.setHostname(ByteString.copyFromUtf8(s"ip-172-31-$i-$j.ec2.internal"))
      .setPort(25520)
      .build()
  }
  val nodeIds = for {
    _ <- 0 until 10000
  } yield {
    NodeId
      .newBuilder()
      .setHigh(Random.nextLong(Long.MaxValue))
      .setLow(Random.nextLong(Long.MaxValue))
      .build()
  }
  val metadata = for {
    _ <- 0 until 10000
  } yield {
    Metadata.newBuilder()
      .putMetadata("roles", ByteString.copyFromUtf8("dc-0"))
      .putMetadata("uid", ByteString.copyFromUtf8(Random.nextLong(Long.MaxValue).toString))
      .build()
  }

  val largeJoinResponse = {
    JoinResponse
      .newBuilder()
      .setSender(Endpoint.newBuilder().setHostname(ByteString.copyFromUtf8("172.31.22.22")).setPort(25520).build())
      .setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
      .setConfigurationId(123456789L)
      .addAllEndpoints(endpoints.asJava)
      .addAllIdentifiers(nodeIds.asJava)
      .addAllMetadataKeys(endpoints.asJava)
      .addAllMetadataValues(metadata.asJava)
      .build()
  }

  println(largeJoinResponse.getSerializedSize)

}
