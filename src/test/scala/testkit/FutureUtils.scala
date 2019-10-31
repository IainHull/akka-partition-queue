package testkit

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.control.{NonFatal, NoStackTrace}
import scala.util.{Failure, Success, Try}

import java.time.Instant
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeoutException

import org.scalactic.source.Position

trait FutureUtils {
  import FutureUtils._

  case class UnrecoverableRetryFailure(message: String) extends Exception(message)

  implicit class RichFuture[A](future: Future[A]) {

    /**
      * Returns the `Future` passed as type class parameter if it completes within the specified timeout, otherwise returns the fallback
      * `Future`. This relies on a Java `Timer` which runs a single thread to handle all calls to this method and does not block.
      *
      * @param timeout time before we return the fallback `Future`
      * @param fallback `Future` returned if the type class parameter Future doesn't complete within the timeout
      * @param ec `ExecutionContext` used to process the `Future`
      *
      * @return the `Future` passed as type class parameter if it completes within the specified timeout, otherwise the fallback `Future`
      */
    def withTimeout(timeout: FiniteDuration, fallback: Future[A])(implicit ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]

      val timerTask = new TimerTask() {
        def run(): Unit = {
          promise.completeWith(fallback)
        }
      }

      timer.schedule(timerTask, timeout.toMillis)

      future
        .map { result =>
          if (promise.trySuccess(result)) {
            timerTask.cancel()
          }
        }
        .recover {
          case NonFatal(t) =>
            if (promise.tryFailure(t)) {
              timerTask.cancel()
            }
        }

      promise.future
    }

    def withTimeout(timeout: FiniteDuration)(implicit pos: Position, ec: ExecutionContext): Future[A] = {
      withTimeout(timeout, Future.failed(new TimeoutException(s"Timeout after $timeout (${pos.fileName}:${pos.lineNumber})")))
    }

    /**
      * Returns the `Future` passed as type class parameter with an added recovery callback which updates the exception message with
      * source location information.
      */
    def withDiagnostic(implicit pos: Position, ec: ExecutionContext): Future[A] = {
      FutureUtils.withDiagnostic(future)
    }

    /**
      * Future that fails if the underlying future succeed before the timeout.  Use this to verify that a message was
      * never sent.  The underlying future can still be used and might succeed after the returned future completes
      *
      * @param timeout the length of time to wait before succeeding
      * @return a future that succeeds when the underlying future never completes within timeout
      */
    def shouldNeverCompleteIn(
        timeout: FiniteDuration
    )(
        implicit pos: Position,
        executor: ExecutionContext
    ): Future[Unit] = {
      case object NeverCompleted extends Exception with NoStackTrace

      val startTime = Instant.now()
      val firstCompleted = withTimeout(timeout, Future.failed(NeverCompleted))
      val handleUnexpected: Future[Unit] = firstCompleted.map { v =>
        val time = Instant.now().toEpochMilli - startTime.toEpochMilli
        throw new Exception(s"Unexpected value $v after $time ms not ${timeout.toMillis} ms (${pos.fileName}:${pos.lineNumber})")
      }
      handleUnexpected.recoverWith {
        case NeverCompleted => Future.successful(())
      }
    }

    /**
      * Future that fails if the underlying future succeed before `notBefore` and does not complete before `notAfter`.
      * Use this to verify the timing that a future completes within the specified time-frame.  The underlying future can still be used
      * and might succeed after the returned future fails.
      *
      * @param notBefore the length to time to wait before succeeding
      * @param notAfter the length of time to wait before failing
      * @return a future completes within the specified time-frame
      */
    def shouldCompleteWithin(
        notBefore: FiniteDuration,
        notAfter: FiniteDuration
    )(
        implicit pos: Position,
        executor: ExecutionContext
    ): Future[A] = {
      val before = shouldNeverCompleteIn(notBefore)
      val after = withTimeout(notAfter)

      before.flatMap(_ => after)
    }
  }

  /**
    * Continue to attempt an operation that returns a `Future`, until you get the desired result. The `until` partial function should be
    * defined for the desired result, this can then map the value. The `Future` returned chains the `Future` from `attemptOp` to each retry,
    * until the `timeout` is reached.
    *
    * @param operation the asynchronous operation to attempt that returns a `Future[A]`
    * @param until a partial function that is defined when the `operation` returns the desired result or the operation fails and should stop
    * @param timeout the total length of time to wait for successful result
    * @param interval the time to wait between retries
    * @param multiplier amount by which to increase interval between retries
    * @param pos the source file position for error messages
    *
    * @tparam A the type of the result of the asynchronous operation
    * @tparam B the type of the result returned eventually
    *
    * @return the retried `Future`
    */
  def retryFutureOperation[A, B](
      operation: => Future[A]
  )(
      until: PartialFunction[A, RetryOp[B]],
      timeout: FiniteDuration,
      interval: FiniteDuration,
      multiplier: Double = 1.0
  )(
      implicit pos: Position,
      ec: ExecutionContext
  ): Future[B] = {
    val start = Instant.now()

    // This function uses Async Continuation Passing Style (ACPS), see http://viktorklang.com/blog/Futures-in-Scala-2.12-part-9.html
    def retryIfRequired(value: Future[A], attempts: Int = 1): Future[B] = {

      def processValue(tryValue: Try[A]): Future[B] = {
        val now = Instant.now()
        val retryOp = tryValue match {
          case Success(s) =>
            until.applyOrElse(s, { _: A =>
              RetryOp.Retry
            })
          case Failure(_) =>
            RetryOp.Retry
        }
        retryOp match {
          case RetryOp.Return(a) =>
            Future.successful(a)
          case RetryOp.Fail(reason) =>
            Future.failed(UnrecoverableRetryFailure(s"Giving up trying: lastValue=$tryValue, reason='$reason'"))
          case RetryOp.Retry if now.isAfter(start.plusMillis(timeout.toMillis)) =>
            val durationMs = s"${(now.until(start, ChronoUnit.MILLIS))}ms"
            val timeoutMs = s"${timeout.toMillis}ms"
            Future.failed(
              new TimeoutException(
                s"Operation timed out: lastValue=$tryValue, attempts=$attempts, duration=$durationMs, timeout=$timeoutMs"
              )
            )
          case RetryOp.Retry =>
            val promise = Promise[B]
            val timerTask = new TimerTask {
              override def run(): Unit = {
                promise.completeWith(retryIfRequired(operation, attempts + 1))
              }
            }
            timer.schedule(timerTask, (interval.toMillis * Math.pow(multiplier, attempts.toDouble)).toLong)
            promise.future
        }
      }

      // Cannot use transformWith as it is not available in scala 2.11
      value
        .flatMap(v => processValue(Success(v)))
        .recoverWith {
          case error: UnrecoverableRetryFailure => Future.failed(error)
          case t: Throwable => processValue(Failure(t))
        }
    }

    retryIfRequired(operation).withDiagnostic
  }

  def withDiagnostic[A](future: Future[A])(implicit pos: Position, ec: ExecutionContext): Future[A] = {
    future.recoverWith {
      case t: Throwable =>
        val diagnosticInfo = s"(${pos.fileName}:${pos.lineNumber})"
        if (t.getMessage.endsWith(diagnosticInfo)) Future.failed(t)
        else Future.failed(new Exception(t.getMessage + " " + diagnosticInfo, t))
    }
  }
}

object FutureUtils extends FutureUtils {
  private val timer: Timer = new Timer(true)

  /**
    * Represents different follow-up actions that should be taken after executing the asynchronous operation in
    * `FutureUtils#retryFutureOperation`.
    *
    * @tparam A type of the eventually returned result
    */
  sealed trait RetryOp[+A]
  object RetryOp {
    final case class Return[A](value: A) extends RetryOp[A]
    case object Retry extends RetryOp[Nothing]
    final case class Fail(reason: String) extends RetryOp[Nothing]
  }
}
