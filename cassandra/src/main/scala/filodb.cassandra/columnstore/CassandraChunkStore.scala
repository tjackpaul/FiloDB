package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.DropStatement
import com.websudos.phantom.connectors.KeySpace
import filodb.cassandra.Util
import filodb.core.Messages._
import filodb.core.Types.ChunkId
import filodb.core.metadata._
import filodb.core.query.ScanInfo
import filodb.core.store.ChunkStore
import filodb.core.util.{FiloLogging, Iterators, MemoryPool}
import scodec.bits.ByteVector

import scala.concurrent.Future

trait CassandraChunkStore extends ChunkStore with MemoryPool with FiloLogging {

  import Util._

  import scala.concurrent.ExecutionContext.Implicits.global

  val cache = Util.lruCache[Projection, ChunkTable](100)
  implicit val keySpace: KeySpace
  implicit val session: Session

  def getChunkTable(projection: Projection): ChunkTable = {
    cache.getOrElse(projection, {
      val chunkTable = new ChunkTable(keySpace, session, projection.dataset, projection.id)
      cache.put(projection, chunkTable)
      chunkTable
    })
  }

  final val META_COLUMN_NAME = "_metadata_"
  final val KEYS_COLUMN_NAME = "_keys_"

  override def deleteProjectionData(projection: Projection): Future[Boolean] = {
    cache.remove(projection)
    val tableName = projection.dataset + projection.id + "_chunks"
    val dropQuery = DropStatement(keySpace.name, tableName, true)
    dropQuery.future().toResponse().map {
      case Success => true
      case _ =>
        metrics.warn(s"Exception occurred while writing to db")
        false
    }
  }

  override def appendChunk(projection: Projection,
                           partition: Any,
                           segment: Any,
                           chunk: ChunkWithMeta): Future[Boolean] = {
    val chunkTable: ChunkTable = getChunkTable(projection)
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString

    val metaDataSize = chunk.metaDataByteSize
    metrics.debug(s"Acquiring buffer of size $metaDataSize for ChunkMetadata")
    val metadataBuf = acquire(metaDataSize)
    val keySize = chunk.keySize(projection.keyType)
    metrics.debug(s"Acquiring buffer of size $keySize for Chunk Key Buffer")
    val keysBuf = acquire(keySize)
    try {
      SimpleChunk.writeMetadata(metadataBuf, chunk.numRows, chunk.chunkOverrides)
      SimpleChunk.writeKeys(keysBuf, chunk.keys, projection.keyType)

      chunkTable.writeChunks(projection, pk,
        segmentId, chunk.chunkId,
        projection.columnNames ++ Seq(META_COLUMN_NAME, KEYS_COLUMN_NAME),
        chunk.columnVectors ++ Seq(metadataBuf, keysBuf))
        .map {
        case Success => true
        case _ =>
          metrics.warn(s"Exception occurred while writing to db")
          false
      }
    } catch {
      case t: Throwable =>
        metrics.warn("Exception occurred while writing buffer", t)
        flow.warn("Exception occurred while writing buffer", t)
        throw t
    } finally {
      release(metadataBuf)
      release(keysBuf)
    }
  }

  import Iterators._

  override def getChunks(scanInfo: ScanInfo): Future[Seq[((Any, Any), Seq[ChunkWithMeta])]] = {
    val columns = scanInfo.columns
    val projection = scanInfo.projection
    val chunkTable: ChunkTable = getChunkTable(projection)
    for {
      metaResult <- chunkTable.getChunkData(scanInfo, META_COLUMN_NAME)

      keysResult <- chunkTable.getDataBySegmentAndChunk(scanInfo, KEYS_COLUMN_NAME)

      dataResult <- Future sequence columns.map { col =>
        chunkTable.getDataBySegmentAndChunk(scanInfo, col)
      }

      segmentChunks = metaResult.map { case (pkBytes, segmentId, chunkId, metadata) =>
        val colBuffers = columns.indices.map(i => dataResult(i)((pkBytes, segmentId, chunkId))).toArray
        val keysBuffer = keysResult((pkBytes, segmentId, chunkId))
        val pType = scanInfo.projection.partitionType
        val pk = pType.fromBytes(ByteVector.view(pkBytes))
        (pk, segmentId, chunkId) -> SimpleChunk(projection, columns, chunkId, colBuffers, keysBuffer, metadata)
      }.iterator


      res = segmentChunks.sortedGroupBy(i => (i._1._1, i._1._2)).toSeq.reverse
        .map { case (segmentId, seq) =>
        (segmentId, seq.map(_._2).toSeq.reverse)
      }
    } yield res
  }

  override def getKeySets(projection: Projection,
                          partition: Any,
                          segment: Any,
                          columns: Seq[String],
                          chunkIds: Seq[ChunkId]): Future[Seq[(ChunkId, Seq[_])]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T])._2.toByteBuffer
    val segmentId = segment.toString
    val chunkTable: ChunkTable = getChunkTable(projection)
    chunkTable.
      getColumnData(projection, pk,
        KEYS_COLUMN_NAME,
        segmentId,
        chunkIds.toList).map { seq =>
      seq.map { case (chunkId, bb) =>
        (chunkId, SimpleChunk.keysFromByteBuffer(bb, projection.keyType))
      }
    }
  }

}
