import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.Scheduler
import akka.util.Timeout
import org.scalactic.source.Position
import testkit.{AsyncActorLoanSpec, AsyncTestProbe}

class QueueActorSpec extends ProtocolSpec(QueueActor)
class PartitionQueueActorSpec extends ProtocolSpec(PartitionQueueActor)

class ProtocolSpec(val protocol: Protocol) extends AsyncActorLoanSpec {
  import protocol._

  behavior of "QueueActor"

  it should "enqueue jobs" in {
    withActors(Fixture()) { fixture =>
      import fixture._

      for {
        result <- askPublish("Foo")
      } yield {
        result shouldBe PublishSucceeded
      }
    }
  }

  it should "launch enqueued jobs" in {
    withActors(Fixture()) { fixture =>
      import fixture._

      for {
        result <- askPublish("Foo")
        str    <- probe.nextMessage
      } yield {
        result shouldBe PublishSucceeded
        str shouldBe "Foo"
      }
    }
  }

  it should "apply back pressure when overloaded" in {
    withActors(Fixture(delay = 200.millis)) { fixture =>
      import fixture._

      for {
        r1 <- askPublish("Foo1")
        r2 <- askPublish("Foo2")
        r3 <- askPublish("Foo3")
        r4 <- askPublish("Foo4")
        r5 <- askPublish("Foo5")
        r6 <- askPublish("Foo6")
        r7 <- askPublish("Foo7")
        r8 <- askPublish("Foo8")
        r9 <- askPublish("Foo9")
        r10 <- askPublish("Foo10")
        r11 <- askPublish("Foo11")

        strs <- probe.nextMessages(10)(Timeout(5.seconds), implicitly, implicitly)
      } yield {
        Seq(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10) should contain only PublishSucceeded
        r11 shouldBe PublishFailed
        strs shouldBe (1 to 10).map(i => s"Foo$i")
      }
    }
  }


  it should "apply back pressure when overloaded 2" in {
    withActors(Fixture(delay = 200.millis)) { fixture =>
      import fixture._

      for {
        rs <- Future.sequence(1 to 11 map { i => askPublish(s"Foo$i") } )
        ss <- probe.nextMessages(10)(Timeout(5.seconds), implicitly, implicitly)
        _  <- probe.expectNoMessage(100.millis)
      } yield {
        rs.slice(0, 10) should contain only PublishSucceeded
        rs.slice(10, 11) should contain only PublishFailed
        ss shouldBe (1 to 10).map(i => s"Foo$i")
      }
    }
  }


  case class Fixture(delay: Duration = Duration.Zero)(context: ActorContext[FixtureCommand]) {
    implicit val scheduler: Scheduler = context.system.scheduler

    // Create a test probe to receive work
    val probe = AsyncTestProbe[String]()
    val probeRef = context.spawnAnonymous(probe.behavior)

    // Create the QueueActor
    val config = Config(1.second, 1.second, 10, 10)
    val partIdOf: String => PartId = _.substring(0, 1)
    val action: String => Unit = { str =>
      logger.info(s"Doing $str (delay: $delay)")
      Thread.sleep(delay.toMillis)
      probeRef ! str
    }

    val queueActor = context.spawn(apply[String](config, partIdOf, action), "queue")

    def askPublish(value: String)(implicit pos: Position): Future[protocol.PublishResponse] = withDiagnostic {
      queueActor ? (sender => Publish(value, sender))
    }
  }
}
