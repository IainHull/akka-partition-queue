import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import java.util.concurrent.Executors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.dispatch.ExecutionContexts
import com.typesafe.scalalogging.StrictLogging

object PartitionQueueActor extends Protocol with StrictLogging {

  sealed trait Command[A]
  object Command {
    final case class PublishJob[A](a: A, sender: ActorRef[PublishResponse]) extends Command[A]
    final case class PartitionIdle[A](partId: PartId) extends Command[A]
  }

  override def Publish[A](a: A, sender: ActorRef[PublishResponse]): Command[A] = Command.PublishJob[A](a, sender)

  override def apply[A](config: Config, partIdOf: A => PartId, action: A => Unit): Behavior[Command[A]] = {

    implicit val executionContext = ExecutionContexts.fromExecutor(Executors.newFixedThreadPool(config.maxConcurrency))

    def run(queues: Map[PartId, ActorRef[PartitionCommand[A]]]): Behavior[Command[A]] = {
      import Command._

      def newPartition(context: ActorContext[Command[A]], partId: PartId): ActorRef[PartitionCommand[A]] = {
        context.spawn(partition(context.self, partId, config, action), partId)
      }

      Behaviors.receive {
        case (context, PublishJob(a, sender)) =>
          val partId = partIdOf(a)
          val partRef = queues.getOrElse(partIdOf(a), newPartition(context, partIdOf(a)))
          partRef ! PartitionCommand.PublishJob(a, sender)
          run(queues + (partId -> partRef))

        case (context, PartitionIdle(partId)) =>
          queues.get(partId).foreach(context.stop(_))
          run(queues - partId)
      }
    }
    run(Map())
  }

  sealed trait PartitionCommand[+A]
  object PartitionCommand {
    final case class PublishJob[A](a: A, sender: ActorRef[PublishResponse]) extends PartitionCommand[A]
    case object JobComplete extends PartitionCommand[Nothing]
    case object Idle extends PartitionCommand[Nothing]
  }

  private def partition[A](parent: ActorRef[Command[A]], partId: PartId, config: Config, action: A => Unit)(implicit executionContext: ExecutionContext): Behavior[PartitionCommand[A]] = {
    import PartitionCommand._

    def launchAndNotify(a: A, self: ActorRef[PartitionCommand[A]]): Future[Unit] = {
      Future {
        logger.info(s"Launching $a")
        action(a)
        logger.info(s"Completing $a")
      }.transform { result =>
        self ! JobComplete
        result
      }
    }

    def run(queue: Queue[A]): Behavior[PartitionCommand[A]] = {
      Behaviors.setup { context =>
        def launch[T](a: A): Future[Unit] = Future(action(a))(executionContext)
        def launchAndNotify(a: A): Unit = context.pipeToSelf(launch(a))(_ => JobComplete)

        Behaviors.withTimers { timers =>
          Behaviors.receiveMessage {
            case PublishJob(a, sender) =>
              logger.info(s"Publish $a ${context.self.path.name}")

              if (queue.size < config.maxPending) {
                if (queue.isEmpty) launchAndNotify(a)
                timers.cancel(Idle)
                sender ! PublishSucceeded
                run(queue.appended(a))
              } else {
                sender ! PublishFailed
                Behavior.same
              }

            case JobComplete =>
              logger.info(s"Complete $queue")
              queue.dequeueOption match {
                case None =>
                  Behavior.same
                case Some((_, queue)) =>
                  queue.headOption match {
                    case Some(a) => launchAndNotify(a)
                    case None => timers.startSingleTimer(Idle, Idle, config.idleTTL)
                  }
                  run(queue)
              }

            case Idle =>
              if (queue.nonEmpty) parent ! Command.PartitionIdle(partId)
              Behavior.same
          }
        }
      }
    }

    run(Queue())
  }
}