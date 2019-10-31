package testkit

import java.lang.management.ManagementFactory

import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

/**
  * Base class for Scalatest unit tests (see: http://www.scalatest.org/user_guide/defining_base_classes).
  */
class BaseSpec extends FlatSpec with CommonTestHelpers {
  import BaseSpec._

  override protected def withFixture(test: NoArgTest): Outcome = {
    logger.info(s"Started test '${test.name}'")

    val startGcInfos = currentGcInfo()

    val outcome = {
      super.withFixture(test) match {
        case exceptional: Exceptional =>
          val endGcInfos = currentGcInfo()
          val diffGcInfos = diffGcInfo(startGcInfos, endGcInfos)

          logger.error(s"GC Information for failed test ${test.name}")
          diffGcInfos.foreach {
            case (pool, gc) => logger.error(s"GC $pool: count=${gc.count} time=${gc.time} millis")
          }
          exceptional
        case other =>
          other
      }
    }

    logger.info(s"Completed test '${test.name}'")

    outcome
  }
}

object BaseSpec {
  final case class GcInfo(count: Long, time: Long)

  def currentGcInfo(): Map[String, GcInfo] = {
    import scala.jdk.CollectionConverters._

    val gcInfos = ManagementFactory.getGarbageCollectorMXBeans.asScala.map { bean =>
      bean.getName -> GcInfo(bean.getCollectionCount, bean.getCollectionTime)
    }.toSeq

    Map(gcInfos: _*)
  }

  def diffGcInfo(start: Map[String, GcInfo], end: Map[String, GcInfo]): Map[String, GcInfo] = {
    val diffs = end.keys.toSeq.map { key =>
      val s = start.getOrElse(key, GcInfo(0, 0))
      val e = end.getOrElse(key, GcInfo(0, 0))

      key -> GcInfo(e.count - s.count, e.time - s.time)
    }

    Map(diffs: _*)
  }
}

trait CommonTestHelpers extends Matchers with OptionValues with TryValues with ScalaFutures with Eventually with Inside with StrictLogging
