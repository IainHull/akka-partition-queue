import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.util.Try

import java.util.concurrent.Executors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.dispatch.ExecutionContexts
import com.typesafe.scalalogging.StrictLogging

object QueueActor extends Protocol with StrictLogging {

  sealed trait Command[A]

  final case class PublishJob[A](a: A, sender: ActorRef[PublishResponse]) extends Command[A]
  final case class JobComplete[A](partId: PartId) extends Command[A]

  override def Publish[A](a: A, sender: ActorRef[PublishResponse]): Command[A] = PublishJob[A](a, sender)


  override def apply[A](config: Config, partIdOf: A => PartId, action: A => Unit): Behavior[Command[A]] = {
    val executionContext = ExecutionContexts.fromExecutor(Executors.newFixedThreadPool(config.maxConcurrency))
    val emptyQueue = Queue[A]()

    def run(queues: Map[PartId, Queue[A]]): Behavior[Command[A]] = {
      Behaviors.setup { context =>
        def queueOf(partId: PartId): Queue[A] = queues.getOrElse(partId, emptyQueue)
        def launch[T](a: A): Future[Unit] = Future(action(a))(executionContext)
        def notify(partId: PartId): Try[Unit] => Command[A] = _ => JobComplete[A](partId)
        def launchAndNotify(a: A): Unit = context.pipeToSelf(launch(a))(notify(partIdOf(a)))

        Behaviors.receiveMessage {
          case PublishJob(a, sender) =>
            logger.info(s"Publish $a $queues")

            val partId = partIdOf(a)
            val queue = queueOf(partId)

            if (queue.size < config.maxPending) {
              if (queue.isEmpty) launchAndNotify(a)
              sender ! PublishSucceeded
              run(queues + (partId -> queue.appended(a)))
            } else {
              sender ! PublishFailed
              Behavior.same
            }

          case JobComplete(partId) =>
            logger.info(s"Complete $partId $queues")
            queueOf(partId).dequeueOption match {
              case None =>
                Behavior.same
              case Some((_, queue)) =>
                queue.headOption.foreach(launchAndNotify(_))
                run(queues + (partId -> queue))
            }
        }
      }
    }

    run(Map())
  }
}
