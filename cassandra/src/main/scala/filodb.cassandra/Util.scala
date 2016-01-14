package filodb.cassandra

import java.nio.ByteBuffer

import com.datastax.driver.core.exceptions.DriverException
import com.websudos.phantom.dsl._
import filodb.core.Messages._

import scala.collection.mutable
import scala.concurrent.Future

/**
 * Utilities for dealing with Cassandra I/O
 */
object Util {

  implicit class ResultSetToResponse(f: Future[ResultSet]) {
    def toResponse(notAppliedResponse: Response = NotApplied): Future[Response] = {
      f.map { resultSet =>
        if (resultSet.wasApplied) Success else notAppliedResponse
      }.recover {
        case e: DriverException => throw StorageEngineException(e)
      }
    }
  }

  implicit class HandleErrors[T](f: Future[T]) {
    def handleErrors: Future[T] = f.recover {
      case e: DriverException => throw StorageEngineException(e)
      // from invalid Enum strings, which should never happen, or some other parsing error
      case e: NoSuchElementException => throw MetadataException(e)
      case e: IllegalArgumentException => throw MetadataException(e)
    }
  }

  def toHex(bb: ByteBuffer): String = com.datastax.driver.core.utils.Bytes.toHexString(bb)

  import scala.collection.JavaConverters._

  def lruCache[A, B](maxEntries: Int): mutable.Map[A, B] =
    new java.util.LinkedHashMap[A, B]() {
      override def removeEldestEntry
      (eldest: java.util.Map.Entry[A, B]):Boolean = size > maxEntries
    }.asScala
}
