/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.Executor

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

package object rapid {

  def toScalaFuture[T](lFuture: ListenableFuture[T], executor: Executor): Future[T] = {
    val p = Promise[T]
    Futures.addCallback(lFuture,
      new FutureCallback[T] {
        def onSuccess(result: T) = p.success(result)
        def onFailure(t: Throwable) = p.failure(t)
      }, executor)
    p.future
  }
}
