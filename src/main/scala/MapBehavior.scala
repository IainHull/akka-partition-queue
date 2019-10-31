import akka.actor.typed.{Behavior, BehaviorInterceptor, Signal, TypedActorContext}
import akka.actor.typed.scaladsl.Behaviors

object MapBehavior {

    def apply[A, B](behavior: Behavior[B])(mapper: A =>B): Behavior[A] = {
      val interceptor = new MapBehaviorInterceptor[A, B](mapper)
      Behaviors.intercept(interceptor)(behavior)
    }

  private final class MapBehaviorInterceptor[A, B](val mapper: A =>B) extends BehaviorInterceptor[A, B] {

    import BehaviorInterceptor._

    override def aroundReceive(ctx: TypedActorContext[A], msg: A, target: ReceiveTarget[B]): Behavior[B] = {
      target(ctx, mapper(msg))
    }

    def aroundSignal(ctx: TypedActorContext[A], signal: Signal, target: SignalTarget[B]): Behavior[B] = {
      target(ctx, signal)
    }

    override def toString: String = "MapBehaviorInterceptor"

    override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = {
      other match {
        case mhi: MapBehaviorInterceptor[_, _] => mapper == mhi.mapper
        case _ => super.isSame(other)
      }
    }
  }
}
