import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.{ActorRef, Behavior}

trait Protocol {
  type PartId = String
  type Command[A]

  case class Config(userTTL: FiniteDuration, idleTTL: FiniteDuration, maxPending: Int, maxConcurrency: Int)

  def Publish[A](a: A, sender: ActorRef[PublishResponse]): Command[A]

  sealed trait PublishResponse
  case object PublishSucceeded extends PublishResponse
  case object PublishFailed extends PublishResponse

  def apply[A](config: Config, partIdOf: A => PartId, action: A => Unit): Behavior[Command[A]]
}
