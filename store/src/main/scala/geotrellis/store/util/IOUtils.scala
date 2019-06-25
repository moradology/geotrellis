/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.store.util

import cats.effect._
import cats.syntax.all._
import cats.syntax.apply

import scala.concurrent.duration._
import scala.util.Random

import geotrellis.util.BlockingThreadPool

object IOUtils {
  /**
    * Implement non-blocking Exponential Backoff on a Task.
    * @param  p  returns true for exceptions that trigger a backoff and retry
    */

  implicit class IOBackoff[A, F[_]: Effect: Timer: Sync](ioa: F[A]) {
    def retryEBO(p: (Throwable => Boolean)): F[A] = {
      def help(count: Int): F[A] = {
        val base: Duration = 52.milliseconds
        val timeout = base * Random.nextInt(math.pow(2, count).toInt) // .extInt is [), implying -1
        val actualDelay = FiniteDuration(timeout.toMillis, MILLISECONDS)

        ioa.handleErrorWith { error =>
          if(p(error)) implicitly[Timer[F]].sleep(actualDelay) *> help(count + 1)
          else implicitly[Sync[F]].raiseError(error)
        }
      }
      help(0)
    }
  }

  def parJoin[K, V](
    ranges: Iterator[(BigInt, BigInt)],
    threads: Int
   )(readFunc: BigInt => Vector[(K, V)]): Vector[(K, V)] =
    parJoinEBO[K, V](ranges, threads)(readFunc)(_ => false)

  private[geotrellis] def parJoinEBO[K, V](
    ranges: Iterator[(BigInt, BigInt)],
    threads: Int
  )(readFunc: BigInt => Vector[(K, V)])(backOffPredicate: Throwable => Boolean): Vector[(K, V)] = {
    // TODO: remove the implicit on ec and consider moving the implicit timer to method signature
    implicit val ec = BlockingThreadPool.executionContext
    implicit val timer: Timer[IO] = IO.timer(ec)
    implicit val cs = IO.contextShift(ec)

    val indices: Iterator[BigInt] = ranges.flatMap { case (start, end) =>
      (start to end).toIterator
    }

    val index: fs2.Stream[IO, BigInt] = fs2.Stream.fromIterator[IO, BigInt](indices)

    val readRecord: (BigInt => fs2.Stream[IO, Vector[(K, V)]]) = { index =>
      fs2.Stream eval IO.shift(ec) *> IO {
        readFunc(index)
      }.retryEBO {
        backOffPredicate
      }
    }

    index
      .map(readRecord)
      .parJoin(threads)
      .compile
      .toVector
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
      .flatten
  }
}
