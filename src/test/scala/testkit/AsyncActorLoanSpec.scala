package testkit

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Assertion, FutureOutcome}
import testkit.AsyncTestProbe.ActorTerminated

abstract class AsyncActorLoanSpec extends AsyncBaseSpec with StrictLogging with FutureUtils {
  import AsyncActorLoanSpec._

  implicit val timeout = Timeout(2.seconds)

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(2.seconds, 20.millis)

  def retry(implicit pc: PatienceConfig): RetryingFutures.Strategy =
    RetryingFutures.Strategy
      .periodic(pc.interval)
      .withTimeout(pc.timeout * 2)

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    complete {
      // Warm up the event handling mechanisms to prevent timing anomalies in tests
      logger.info(s"Started test '${test.name}'")

      val future = super
        .withFixture(test)
        .toFuture

      new FutureOutcome(future)
    }.lastly {
      logger.info(s"Completed test '${test.name}'")
    }
  }

  type FixtureCommand = AsyncActorLoanSpec.FixtureCommand

  def withActors[T](factory: ActorContext[FixtureCommand] => T)(testCode: T => Future[Assertion]): Future[Assertion] = {
    val promise = Promise[T]()
    val root = Behaviors.setup[FixtureCommand] { ctx =>
      promise.success(factory(ctx))
      Behaviors.receiveMessage {
        case RestartChild(child, childBehavior, sender) =>
          if (child.path.parent === ctx.self.path) {
            ctx.watchWith(child, RespawnChild(child.path.name, childBehavior, sender))
            ctx.stop(child)
          }
          Behaviors.same
        case RespawnChild(name, childBehavior, sender) =>
          sender.tell(ctx.spawn(childBehavior, name))
          Behaviors.same
        case WatchChild(child, sender) =>
          sender ! AsyncTestProbe.watch(ctx, child)
          Behaviors.same
      }
    }

    val actorSystem = ActorSystem[FixtureCommand](root, "async-actor-spec-root")
    complete {
      promise.future.flatMap { t =>
        testCode(t)
      }
    }.lastly {
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 2.seconds)
    }
  }
}

object AsyncActorLoanSpec {
  sealed trait FixtureCommand
  final case class RestartChild[A](child: ActorRef[A], behavior: Behavior[A], sender: ActorRef[ActorRef[A]]) extends FixtureCommand
  final case class WatchChild[A](child: ActorRef[A], sender: ActorRef[AsyncTestProbe[ActorTerminated]]) extends FixtureCommand
  private final case class RespawnChild[A](name: String, behavior: Behavior[A], sender: ActorRef[ActorRef[A]]) extends FixtureCommand
}
