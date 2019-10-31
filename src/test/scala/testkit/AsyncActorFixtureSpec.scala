package testkit

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.Try

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FutureOutcome
import org.scalatest.fixture.AsyncFlatSpec

abstract class AsyncActorFixtureSpec extends AsyncFlatSpec with CommonTestHelpers with StrictLogging with FutureUtils {

  implicit val timeout: Timeout = Timeout(2.seconds)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(2.seconds, 20.millis)

  def retry(implicit pc: PatienceConfig): RetryingFutures.Strategy =
    RetryingFutures.Strategy
      .periodic(pc.interval)
      .withTimeout(pc.timeout * 2)

  def makeFixture(ctx: ActorContext[Nothing]): FixtureParam

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    logger.info(s"Started test '${test.name}'")

    val promise = Promise[FixtureParam]()
    val root = Behaviors.setup[Nothing] { ctx =>
      promise.complete(Try(makeFixture(ctx)))
      Behaviors.empty
    }
    val actorSystem = ActorSystem[Nothing](root, "async-actor-spec-root")

    complete {
      val future = promise.future
        .flatMap { fixture =>
          withFixture(test.toNoArgAsyncTest(fixture)).toFuture
        }
      new FutureOutcome(future)
    }.lastly {
      logger.info(s"Completed test '${test.name}'")
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 2.second)
    }
  }
}
