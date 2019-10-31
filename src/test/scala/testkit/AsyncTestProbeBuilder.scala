package testkit

import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

/**
  * Builds `AsyncTestProbe`s and caches them based on their key. This is generally used to verify actors that create child actors. For
  * instance, if parent actor's `Behavior` is constructed by a method like below:
  * {{{
  *   object ParentActor {
  *     def apply(childFactory: ChildInfo => Behavior[ChildActor.Command]): Behavior[ParentActor.Command] = ...
  *   }
  * }}}
  * it can be instantiated for testing with an `AsyncTestProbeBuilder` for constructing the child actors like this:
  * {{{
  *   val childProbeBuilder = AsyncTestProbeBuilder[ChildInfo, ChildActor.Command]()
  *   context.spawn(ParentActor(childInfo => childProbeBuilder.get(childInfo).behavior), "parent")
  * }}}
  *
  * The key uniquely identifies an instance of a behavior. If the `Behavior`s are parametrized you can construct the builder by passing in a
  * function from key to `Behavior`, in which case `Behavior` parameters serve as a key. If the `Behavior`s don't need to be parametrized
  * simply pass in the `Behavior` that should be used by all created probes.
  *
  * @param customBehaviorFactory function describing how to construct `Behavior`s for the probes based on a key
  *
  * @tparam A the key type
  * @tparam B the type of messages accepted by the created `AsyncTestProbe`
  */
class AsyncTestProbeBuilder[A, B](customBehaviorFactory: A => Behavior[B]) {

  private val map: ConcurrentHashMap[A, AsyncTestProbe[B]] = new ConcurrentHashMap()

  /**
    * Returns an `AsyncTestProbe` for the given key with behavior passed when creating the builder, creating a new probe if required.
    *
    * @param key the key of the test probe
    */
  def get(key: A): AsyncTestProbe[B] = {
    if (!map.contains(key)) map.putIfAbsent(key, AsyncTestProbe[B](customBehaviorFactory(key)))
    map.get(key)
  }
}

object AsyncTestProbeBuilder {
  def apply[A, B](): AsyncTestProbeBuilder[A, B] = new AsyncTestProbeBuilder(_ => Behaviors.receiveMessage[B](_ => Behaviors.unhandled[B]))

  def apply[A, B](customBehavior: Behavior[B]): AsyncTestProbeBuilder[A, B] = new AsyncTestProbeBuilder((_: A) => customBehavior)

  def apply[A, B](customBehaviorFactory: A => Behavior[B]): AsyncTestProbeBuilder[A, B] = new AsyncTestProbeBuilder(customBehaviorFactory)

  def apply[A, B](onMessage: PartialFunction[B, Behavior[B]]): AsyncTestProbeBuilder[A, B] =
    new AsyncTestProbeBuilder(_ => Behaviors.receiveMessagePartial[B](onMessage))
}
