package testkit

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.actor.typed.{ActorRef, Behavior, Terminated}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import org.scalactic.source.Position

/**
  * An actor test probe, that returns its results asynchronously. Each message the actor receives completes a `Promise`. Each time the
  * probe is asked for a message it returns the `Future` for that `Promise`.
  *
  * This class is thread safe. It synchronizes on the addition and removal of `Promise`s, this enables it to return message `Future`s from a
  * different thread to the one that receives the messages (which is an actor). These synchronized sections should have almost no
  * contention and are mainly required for visibility on multi core CPUs.
  *
  * @tparam A the type of messages handled
  */
class AsyncTestProbe[A] private (customBehavior: Behavior[A]) extends StrictLogging {
  import FutureUtils._

  /**
    * Queue of `Promise`s that represent pending message requests from external callers. Whenever a message is received, the first
    * `Promise` in the queue is removed and completed. If there are no `Promise`s in this queue when a message is received (i.e., no
    * pending message requests), a new `Promise` is created and added to the `receivedMessages` queue.
    */
  private val requestedMessages: mutable.Queue[Promise[A]] = mutable.Queue.empty

  /**
    * Queue of `Promise`s that represent received messages. Whenever a message is requested by an external caller, the first `Promise` in
    * this queue is removed and completed. If there are no `Promise`s in this queue when a message is requested (i.e., no messages were
    * received), a new `Promise` is created and added to the `requestedMessages` queue.
    */
  private val receivedMessages: mutable.Queue[Promise[A]] = mutable.Queue.empty

  private def promiseFrom[T](queue: mutable.Queue[Promise[A]])(fn: mutable.Queue[Promise[A]] => T): T = {
    this.synchronized {
      if (queue.isEmpty) {
        val promise = Promise[A]()
        requestedMessages.enqueue(promise)
        receivedMessages.enqueue(promise)
      }
      fn(queue)
    }
  }

  /**
    * Dequeues the next `Promise` from the specified queue if the queue is not empty. Otherwise, creates a new `Promise` in both queues
    * and then performs a dequeue on the specified queue.
    *
    * Note: This method is synchronized as `receivedMessages` is accessed from callers threads, while `requestedMessages` is accessed from
    * the actor's thread.
    *
    * @param queue the queue to read from
    *
    * @return the next `Promise`
    */
  private def takePromiseFrom(queue: mutable.Queue[Promise[A]]): Promise[A] = {
    promiseFrom(queue)(_.dequeue())
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def peakPromiseFrom(queue: mutable.Queue[Promise[A]]): Promise[A] = {
    promiseFrom(queue)(_.head)
  }

  /**
    * @return a `Future` of the next message this test probe will receive (with implicit timeout)
    */
  def nextMessage(implicit timeout: Timeout, pos: Position, executor: ExecutionContext): Future[A] = {
    withDiagnostic {
      nextMessageNoTimeout.withTimeout(timeout.duration)
    }
  }

  /**
    * @return a `Future` of the next message this test probe will receive cast to a subtype of `A` (with implicit timeout)
    */
  def nextMessageAs[B <: A: ClassTag](implicit timeout: Timeout, pos: Position, executor: ExecutionContext): Future[B] = {
    withDiagnostic {
      nextMessageNoTimeout.mapTo[B].withTimeout(timeout.duration)
    }
  }

  /**
    * @return a `Future` of the next message this test probe will receive, without any timeout added
    */
  def nextMessageNoTimeout: Future[A] = takePromiseFrom(receivedMessages).future

  /**
    * @return a `Future` of the next `n` messages this test probe will receive
    */
  def nextMessages(n: Int)(implicit timeout: Timeout, pos: Position, executor: ExecutionContext): Future[Seq[A]] = {
    nextMessagesNoTimeout(n).withTimeout(timeout.duration)
  }

  /**
    * @return a `Future` of the next `n` messages this test probe will receive
    */
  def nextMessagesNoTimeout(n: Int)(implicit executor: ExecutionContext): Future[Seq[A]] = {
    this.synchronized {
      require(n >= 1, s"Can only ask for one or more messages: requestedMessages=$n")

      val futures = (1 to n).map { _ =>
        takePromiseFrom(receivedMessages).future
      }
      Future.sequence(futures)
    }
  }

  /**
    * Clears the internal queue of received messages and returns a `Future` containing these messages.
    *
    * @return a completed `Future` of all received messages
    */
  def drainMessages(implicit executor: ExecutionContext): Future[Seq[A]] = {
    this.synchronized {
      val promises = receivedMessages.dequeueAll(p => p.future.isCompleted)
      Future.sequence(promises.map(_.future))
    }
  }

  /**
    * Asynchronously asserts that no messages are received by the probe for the specified time.
    *
    * @return a `Future` which succeeds if no messages are received for the specified time or fails if a message is received
    */
  def expectNoMessage(duration: FiniteDuration)(implicit executor: ExecutionContext): Future[Unit] = {
    peakPromiseFrom(receivedMessages).future.shouldNeverCompleteIn(duration)
  }

  /**
    * Keeps fetching messages from the probe until either a message of the specified type is received or the timeout is reached.
    *
    * @return a `Future` which succeeds if a message of the specified type is received within the given time window
    */
  def fishMessage[B <: A: ClassTag](implicit timeout: Timeout, pos: Position, executor: ExecutionContext): Future[B] = {
    def fish: Future[B] = {
      nextMessageNoTimeout.mapTo[B].recoverWith { case _ => fish }
    }

    withDiagnostic {
      fish.withTimeout(timeout.duration)
    }
  }

  /**
    * Returns the behaviour of this test probe.
    *
    * @return the behavior of this test probe
    */
  val behavior: Behavior[A] = {
    // The trick below is necessary because it's impossible to use `orElse` directly on `customBehavior` if it's a `DeferredBehavior`
    // (e.g., behavior returned by `Behaviors.setup`). In such cases, `DeferredBehavior` needs to be "unwrapped" or "started" and only then
    // composed with the fallback behavior. To preserve the deferred nature of `DeferredBehavior`s the composed `Behavior` is wrapped in
    // another `DeferredBehavior.`
    Behaviors.setup { context =>
      val started = Behavior.validateAsInitial(Behavior.start(customBehavior, context))
      started.orElse {
        Behaviors.receiveMessage { message =>
          logger.debug(s"Stashing message: probe=${context.self.path.name}, message=$message")
          takePromiseFrom(requestedMessages).success(message)
          Behaviors.same
        }
      }
    }
  }
}

object AsyncTestProbe extends StrictLogging {

  /**
    * Creates an `AsyncTestProbe` with behavior based on the passed `PartialFunction`. The `onMessage` can be used to inject custom
    * behavior for certain messages before completing next promise (e.g., this allows to create test probes that stop or ignore messages).
    * When implementing `onMessage` keep in mind that a message will complete a `Future` returned by `nextMessage` (i.e., be visible for
    * the test using the probe) only when `onMessage` is undefined for that message or returns `Behaviors.unhandled`. Otherwise the test
    * probe actor will use the behavior returned by `onMessage` and not store the message. Returning `Behaviors.empty` turns the actor
    * into a lame duck and is a little stupid.
    *
    * @param onMessage message handling function that adds custom logic to the probe
    *
    * @tparam A the type of messages handled
    *
    * @return a new `AsyncTestProbe`
    */
  def fromPartial[A](onMessage: PartialFunction[A, Behavior[A]]): AsyncTestProbe[A] = apply(Behaviors.receiveMessagePartial(onMessage))

  def apply[A](customBehavior: Behavior[A] = noCustomBehavior[A]): AsyncTestProbe[A] = new AsyncTestProbe[A](customBehavior)

  private def noCustomBehavior[A]: Behavior[A] = Behaviors.receiveMessage(_ => Behaviors.unhandled[A])

  /**
    * Creates a new `AsyncTestProbe` that watches the supplied actors and automatically creates an anonymous actor in the supplied context
    * using the probe's `Behavior`. The `Terminated` signals generated when the watched actors shut down are mapped into
    * `ActorTerminated` messages and available through `Future`s returned by `nextMessage` calls on the probe.
    *
    * @param context the context to create the probe actor in
    * @param ref1 the first actor to watch
    * @param refs the subsequent actors to watch
    *
    * @return the watching test probe
    */
  def watch[A](context: ActorContext[A], ref1: ActorRef[Nothing], refs: ActorRef[Nothing]*): AsyncTestProbe[ActorTerminated] = {
    val probe = AsyncTestProbe.fromPartial[ActorTerminated](PartialFunction.empty)

    // The anonymous actor never terminates, but since this is designed for unit tests it's fine
    context.spawnAnonymous[Nothing](
      Behaviors.setup[Nothing] { setupContext =>
        val probeRef = setupContext.spawnAnonymous(probe.behavior)
        (ref1 +: refs).foreach { ref =>
          setupContext.watch(ref)
        }

        Behaviors.receiveSignal[Nothing] {
          case (_, terminated: Terminated) =>
            logger.debug(s"Actor terminated: ref=${terminated.ref}")
            probeRef ! ActorTerminated(terminated.ref)
            Behaviors.same
        }
      }
    )

    probe
  }

  /**
    * Wrapper for the information included in `Terminated` signals. Necessary because it's impossible to explicitly send signals to actors.
    */
  final case class ActorTerminated(ref: ActorRef[Nothing])
}
