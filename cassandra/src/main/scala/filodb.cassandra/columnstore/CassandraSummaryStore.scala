package filodb.cassandra.columnstore

import filodb.cassandra.Util
import filodb.core.Messages.Success
import filodb.core.Types.ChunkId
import filodb.core.metadata.{Projection, SegmentSummary}
import filodb.core.store.SummaryStore
import filodb.core.util.{FiloLogging, MemoryPool}
import filodb.util.TimeUUIDUtils

import scala.concurrent.{ExecutionContext, Future}

trait CassandraSummaryStore extends SummaryStore with FiloLogging with MemoryPool {

  def summaryTable: SummaryTable

  val summaryCache = Util.lruCache[(Projection, Any, Any), (SegmentVersion, SegmentSummary)](100)

  implicit val ec: ExecutionContext

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  override def compareAndSwapSummary(projection: Projection,
                                     partition: Any,
                                     segment: Any,
                                     oldVersion: Option[SegmentVersion],
                                     newVersion: SegmentVersion,
                                     segmentSummary: SegmentSummary): Future[Boolean] = {

    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString
    flow.debug(s"Comparing and setting summary for " +
      s"partition $partition and segment $segmentId " +
      s"with old version $oldVersion and new version $newVersion")
    val segmentSummarySize = segmentSummary.size
    metrics.debug(s"Acquiring buffer of size $segmentSummarySize for SegmentSummary")
    val ssBytes = acquire(segmentSummarySize)
    try {
      SegmentSummary.write(ssBytes, segmentSummary)
      summaryTable.compareAndSetSummary(projection, pk, segmentId,
        oldVersion, newVersion, ssBytes).map {
        case Success =>
          // summaryCache.put((projection, pk, segmentId), (newVersion, segmentSummary))
          true
        case _ => false
      }
    } catch {
      case t: Throwable =>
        metrics.warn("Exception occurred while writing buffer", t)
        flow.warn("Exception occurred while writing buffer", t)
        throw t
    } finally {
      release(ssBytes)
    }
  }

  override def readSegmentSummary(projection: Projection,
                                  partition: Any,
                                  segment: Any): Future[Option[(SegmentVersion, SegmentSummary)]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString
    summaryTable.readSummary(projection, pk, segmentId)
    //    val summary = summaryCache.get((projection, pk, segmentId))
    //    summary match {
    //      case Some(obj) =>
    //        flow.debug(s"Fetched cached summary" +
    //          s" for $projection with partition $pk with segment $segmentId")
    //        Future(Some(obj))
    //      case None => summaryTable.readSummary(projection, pk, segmentId)
    //    }
  }

  override def getNewSegmentVersion: SegmentVersion = TimeUUIDUtils.getUniqueTimeUUIDinMillis

  override def nextChunkId(segment: Any): ChunkId = TimeUUIDUtils.getUniqueTimeUUIDinMillis
}
