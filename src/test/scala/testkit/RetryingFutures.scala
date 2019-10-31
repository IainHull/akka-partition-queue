package testkit

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalactic.{Good, Or}
import org.scalactic.source.Position

object RetryingFutures {
  import FutureUtils._

  /**
    * Defines a strategy for retrying futures. Currently supports fixed period and exponential backoff
    * @param initialPeriod the initial period for exponential backoff, the recurring period for fixed retries
    * @param timeout the maximum amount of time to continue attempting retries
    * @param multiplier each retry is initialPeriod * pow(multiplier, n) where n is the attempt number
    */
  final case class Strategy(initialPeriod: FiniteDuration, timeout: FiniteDuration, multiplier: Double) {

    /**
      * Syntactic sugar for changing the retry max wait time
      * @param timeout the new maximum wait time
      * @return the updated strategy
      */
    def withTimeout(timeout: FiniteDuration): Strategy = copy(timeout = timeout)

    /**
      * Encode the retry strategy and the operation to be retried into an executable structure which will perform the retries
      * @param op the by-name operation to execute the future
      * @return the Executable that will perform retries
      */
    def attempt[T](op: => Future[T]): Executable[T] = {
      Executable(this, { () =>
        op
      })
    }

    /**
      * Execute the retry strategy until the future returns sucessfully
      * @param op the by-name operation to execute the future
      * @return the first succesful result of invoking the future op
      */
    def execute[T](op: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      Executable(this, { () =>
        op
      }).until(RetryingFutures.isSuccessful)
    }
  }

  object Strategy {

    /**
      * A periodic retry. Defaults to a max of 10 retries but this can be overridden by calling withTimeout
      * @param period the time to wait between a failure and a retry attempt
      * @return a Strategy which describes a periodic retry
      */
    def periodic(period: FiniteDuration): Strategy = Strategy(period, period * 10, 1)

    /**
      * A retry with exponential backoff. Defaults to a max of 10 retries by this can be overridden by calling withTimeout
      * @param initialPeriod the time to wait after the first failure
      * @param multiplier scaling factor between subsequent retry wait times
      * @return a Strategy which describes an exponential backoff retry
      */
    def exponentialBackoff(initialPeriod: FiniteDuration, multiplier: Double = 1.5): Strategy =
      Strategy(initialPeriod, (initialPeriod.toMillis * math.pow(multiplier, 11)).milliseconds, multiplier)
  }

  /**
    * A class which wraps a retry strategy and a retryable operation which can be used to execute retries
    * @param strategy defines the retry strategy
    * @param op operation to generate the future on each retry
    */
  final case class Executable[T](strategy: Strategy, op: () => Future[T], fail: PartialFunction[T, String] = Map[T, String]()) {

    /**
      * Short-circuits retrying when an unrecoverable error is encountered
      * @param terminationCondition defines the unrecoverable condition and returns the appropriate error message
      * @return a copy of Executable with the termination condition saved
      */
    def failWhen(terminationCondition: PartialFunction[T, String]): Executable[T] = copy(fail = terminationCondition)

    /**
      * Execute the future operation until the future value is a valid input for the terminationCondition partial function
      * The value returned by the partial function is then returned by this method
      * @param terminationCondition defines when retries should stop and maps the resultant value
      * @return the future value mapped through the terminationCondition partial function
      */
    def until[R](terminationCondition: PartialFunction[T, R])(implicit ec: ExecutionContext, pos: Position): Future[R] = {
      FutureUtils.retryFutureOperation(op())(
        until = {
          case result =>
            fail.lift(result) match {
              case Some(error) =>
                RetryOp.Fail(s"$error ${pos.fileName}:${pos.lineNumber}")
              case None =>
                terminationCondition.lift(result) match {
                  case Some(r) => RetryOp.Return(r)
                  case None => RetryOp.Retry
                }
            }
        },
        timeout = strategy.timeout,
        interval = strategy.initialPeriod,
        multiplier = strategy.multiplier
      )
    }
  }

  /**
    * Helper partial function that stops retrying as soon as the future completes successfully
    * @return the future value, unmodified
    */
  def isSuccessful[T]: PartialFunction[T, T] = {
    case x => x
  }

  /**
    * Helper partial function that stops retrying when the future value is the Good branch of a scalactic Or
    * @return extracts the Good value from inside the Or
    */
  def isGood[T]: PartialFunction[Or[T, _], T] = {
    case Good(x) => x
  }

  /**
    * Helper partial function that stops retrying when the future value satisfies the given predicate
    * @param predicate the predicate function which must be satisfied by the future value
    * @return the future value, unmodified
    */
  def predicateMatches[T](predicate: T => Boolean): PartialFunction[T, T] = {
    case t if predicate(t) => t
  }

}
