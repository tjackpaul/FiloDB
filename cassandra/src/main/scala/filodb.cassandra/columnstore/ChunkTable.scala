package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.websudos.phantom.TokenRangeClause
import com.websudos.phantom.builder.query.SelectQuery
import com.websudos.phantom.builder.{Chainned, Unlimited, Unordered, Unspecified}
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import filodb.cassandra.query.{SegmentedTokenRangeScanInfo, TokenRangeScanInfo}
import filodb.core.Messages.Response
import filodb.core.Types.ChunkId
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{PartitionScanInfo, ScanInfo, SegmentedPartitionScanInfo}
import filodb.core.util.FiloLogging
import shapeless.HNil

import scala.concurrent.Future

/**
 * Represents the table which holds the actual columnar chunks for segments of a projection.
 * Each row stores data for a column (chunk) of a segment.
 *
 */
sealed class ChunkTable(ks: KeySpace, _session: Session, val dataset: String, val projectionId: Int)
  extends CassandraTable[ChunkTable, (String, ChunkId, String, ByteBuffer)]
  with FiloLogging {


  import filodb.cassandra.Util._

  import scala.language.postfixOps

  implicit val keySpace = ks
  implicit val session = _session

  override val tableName = dataset + projectionId + "_chunks"

  //scalastyle:off

  object partition extends BlobColumn(this) with PartitionKey[ByteBuffer]

  object columnName extends StringColumn(this) with ClusteringOrder[String]

  object segmentId extends StringColumn(this) with ClusteringOrder[String]

  object chunkId extends TimeUUIDColumn(this) with ClusteringOrder[UUID]

  object data extends BlobColumn(this)

  //scalastyle:on

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def writeChunks(projection: Projection,
                  partition: ByteBuffer,
                  segmentId: String,
                  chunkId: ChunkId,
                  columnNames: Seq[String],
                  columnVectors: Seq[ByteBuffer]): Future[Response] = {
    flow.debug(s"Writing chunk with segment $segmentId and chunk $chunkId")
    val insertQ = insert.value(_.partition, partition)
      .value(_.segmentId, segmentId)
      .value(_.chunkId, chunkId)
    // NOTE: This is actually a good use of Unlogged Batch, because all of the inserts
    // are to the same partition key, so they will get collapsed down into one insert
    // for efficiency.
    // NOTE2: the batch add is immutable, so use foldLeft to get the updated batch
    val batchQuery = columnVectors.zipWithIndex.foldLeft(Batch.unlogged) {
      case (batch, (bytes, i)) =>
        val columnName = columnNames(i)
        batch.add(insertQ.value(_.columnName, columnName)
          .value(_.data, bytes))
    }
    batchQuery.future().toResponse()
  }

  def getDataBySegmentAndChunk(scanInfo: ScanInfo,
                               columnName: String): Future[Map[(ByteBuffer, String, ChunkId), ByteBuffer]] = {
    for {
      result <- getChunkData(scanInfo, columnName)
      // there would be a unique combination of segmentId and chunkId.
      // so we just pick up the only value
      data = result.groupBy(i => Tuple3(i._1, i._2, i._3)).mapValues(l => l.head._4)
    } yield data
  }

  def getChunkData(scanInfo: ScanInfo,
                   columnName: String): Future[Seq[(ByteBuffer, String, UUID, ByteBuffer)]] = {

    scanInfo match {

      case PartitionScanInfo(projection, c, partition) =>
        val pType = projection.partitionType
        val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
        select(_.partition, _.segmentId, _.chunkId, _.data)
          .where(_.partition eqs pk)
          .and(_.columnName eqs columnName)
          .fetch()

      case SegmentedPartitionScanInfo(projection, c, partition, segmentRange) =>
        val pType = projection.partitionType
        val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
        var query = select(_.partition, _.segmentId, _.chunkId, _.data)
          .where(_.partition eqs pk)
          .and(_.columnName eqs columnName)
        query = appendSegmentRange(segmentRange, query)
        query
          .fetch()

      case TokenRangeScanInfo(projection, c, tokenRange) =>
        select(_.partition, _.segmentId, _.chunkId, _.data)
          .where(t => TokenRangeClause.tokenGt(t.partition.name, tokenRange.start))
          .and(t => TokenRangeClause.tokenLte(t.partition.name, tokenRange.end))
          .and(_.columnName eqs columnName)
          .allowFiltering().fetch()

      case SegmentedTokenRangeScanInfo(projection, c, tokenRange, segmentRange) =>
        var query = select(_.partition, _.segmentId, _.chunkId, _.data)
          .where(t => TokenRangeClause.tokenGt(t.partition.name, tokenRange.start))
          .and(t => TokenRangeClause.tokenLte(t.partition.name, tokenRange.end))
          .and(_.columnName eqs columnName)
        query = appendSegmentRange(segmentRange, query)
        query
          .allowFiltering().fetch()
    }
  }


  private def appendSegmentRange(segmentRange: KeyRange[_],
                                 query: SelectQuery[ChunkTable,
                                   (ByteBuffer, String, ChunkId, ByteBuffer),
                                   Unlimited,
                                   Unordered,
                                   Unspecified,
                                   Chainned, HNil]) = {
    val start =
      if (segmentRange.start.isDefined) {
        if (segmentRange.startExclusive) {
          query.and(_.segmentId gt segmentRange.start.get.toString)
        } else {
          query.and(_.segmentId gte segmentRange.start.get.toString)
        }
      } else {
        query
      }

    if (segmentRange.end.isDefined) {
      if (segmentRange.endExclusive) {
        start.and(_.segmentId lt segmentRange.end.get.toString)
      }
      else {
        start.and(_.segmentId lte segmentRange.end.get.toString)
      }
    } else {
      start
    }


  }

  def getColumnData(projection: Projection,
                    partition: ByteBuffer,
                    columnName: String,
                    segmentId: String,
                    chunkIds: List[ChunkId]): Future[Seq[(ChunkId, ByteBuffer)]] =
    select(_.chunkId, _.data)
      .where(_.partition eqs partition)
      .and(_.columnName eqs columnName)
      .and(_.segmentId eqs segmentId)
      .and(_.chunkId in chunkIds).fetch()


  override def fromRow(r: Row): (String, ChunkId, String, ByteBuffer) = {
    (segmentId(r), chunkId(r), columnName(r), data(r))
  }
}
