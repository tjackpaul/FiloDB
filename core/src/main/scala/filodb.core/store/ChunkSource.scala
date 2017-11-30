package filodb.core.store

import java.nio.ByteBuffer

import kamon.Kamon
import monix.reactive.Observable

import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.query.ChunkSetReader
import filodb.core.Types.PartitionKey

/**
 * ChunkSource is the base trait for a source of chunks given a `PartitionScanMethod` and a
 * `ChunkScanMethod`.  It is the basis for querying and reading out of raw chunks.
 *
 * Besides the basic methods here, see `package.scala` for derivative methods including aggregate
 * and row iterator based methods (intended for things like Spark)
 */
trait ChunkSource {
  def stats: ChunkSourceStats

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * Used only for something like Spark that has to distribute scans across a cluster.
   * @param dataset the name of the dataset to determine splits for
   * @param splitsPerNode the number of splits to target per node.  May not actually be possible.
   * @return a Seq[ScanSplit]
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit]

  /**
   * Scans and returns partitions according to the method.  Note that the Partition returned may not
   * contain any data; an additional streamReaders method must be invoked to actually return chunks.
   * This allows data to be returned lazily for systems like Cassandra.
   * @param dataset the Dataset to read from
   * @param partMethod which partitions to scan
   * @return an Observable over FiloPartition
   */
  def scanPartitions(dataset: Dataset,
                     partMethod: PartitionScanMethod): Observable[FiloPartition]

  /**
   * Reads chunks from a dataset and returns an Observable of chunk readers.
   *
   * @param dataset the Dataset to read from
   * @param columnIDs the set of column IDs to read back.  Order determines the order of columns read back
   *                in each row.  These are the IDs from the Column instances.
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable of ChunkSetReaders
   */
  def readChunks(dataset: Dataset,
                 columnIDs: Seq[Types.ColumnId],
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ChunkSetReader] = {
    val ids = columnIDs.toArray
    scanPartitions(dataset, partMethod)
      .flatMap { partition =>
        stats.incrReadPartitions(1)
        partition.streamReaders(chunkMethod, ids)
      }
  }

  /**
    * Quickly retrieves all the partition keys this chunk source has stored for the given shard number.
    * Useful when the node has just restarted and all the available partitions for the shard needs to be
    * read so query indexes can be populated.
    */
  def scanPartitionKeys(dataset: Dataset, shardNum: Int): Observable[PartitionKey]
}

/**
 * Statistics for a ChunkSource.  Some of this is used by unit tests.
 */
class ChunkSourceStats {
  private val readPartitionsCtr  = Kamon.metrics.counter("read-partitions")
  private val readChunksetsCtr   = Kamon.metrics.counter("read-chunksets")
  private val chunkNoInfoCtr     = Kamon.metrics.counter("read-chunks-with-no-info")
  var readChunkSets: Int = 0
  var readPartitions: Int = 0

  def incrReadPartitions(numPartitions: Int): Unit = {
    readPartitionsCtr.increment(numPartitions)
    readPartitions += numPartitions
  }

  def incrReadChunksets(): Unit = {
    readChunksetsCtr.increment
    readChunkSets += 1
  }

  def incrChunkWithNoInfo(): Unit = { chunkNoInfoCtr.increment }
}

final case class SingleChunkInfo(id: Types.ChunkID, colNo: Int, bytes: ByteBuffer)
