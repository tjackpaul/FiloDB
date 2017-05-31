package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.Kamon
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.language.existentials

import filodb.coordinator.client.DatasetOps
import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.memstore.{MemStore, IngestRecord, DatasetAlreadySetup}

object RowSource {
  case object GetMoreRows
  case object AllDone
  case object CheckCanIngest
  case object AckTimeout
  case class SetupError(err: ErrorResponse)
  case class IngestionErr(msg: String, cause: Option[Throwable] = None)
}

/**
 * A zero-arg constructor class that knows how to create a RowSource.
 */
trait RowSourceFactory {
  /**
   * Returns the Props for creating a RowSource actor
   * @param config the configuration for the data source
   * @param projection
   * @param memStore
   */
  def create(config: Config,
             projection: RichProjection,
             memStore: MemStore): Props
}

/**
 * RowSource is a trait to make it easy to write sources (Actors) for specific
 * input methods - eg from HTTP JSON, or CSV, or Kafka, etc.
 * It has logic to handle flow control/backpressure.
 * It is responsible for reading rows from the source and sending it to different FiloDB nodes and
 * their NodeCoordinator's, routing the data according to the latest ShardMap.
 *
 * It is started by sending it an initial ShardMap.  This normally does not need to be done by the user,
 * as RowSources are normally started by MemStoreCoordActors which will feed it ShardMap updates.
 * Start ingestion on all nodes by sending SetupDataset to NodeClusterActor.
 *
 * Backpressure and at least once mechanism:  RowSource sends batches of rows to the NodeCoordinator,
 * but does not send more than maxUnackedBatches before getting an ack back.  As long as it receives
 * acks it will keep sending. If no acks are received in ackTimeout, it goes into a waiting state, checking
 * if it can replay messages, replaying them, and waiting for acks for all of them to be received.
 * At that point it will go back into regular reading mode.
 * If the memtable is full, then we receive a Nack, and also go into waiting mode, but the coordinator will
 * send us a ResumeIngest message so we know when to start sending again.
 *
 * ackTimeouts should be an exceptional event, not a regular occurrence.  Make sure to adjust for network
 * response times.
 */
trait RowSource extends Actor with StrictLogging {
  import RowSource._

  // Maximum number of unacked batches to push at a time.  Used for flow control.
  def maxUnackedBatches: Int

  def waitingPeriod: FiniteDuration = 5.seconds
  def ackTimeout: FiniteDuration = 20.seconds

  def projection: RichProjection
  def version: Int

  // Returns newer batches of rows.
  def batchIterator: Iterator[Seq[IngestRecord]]

  // Anything additional to do when we hit end of data and it's all acked, before killing oneself
  def allDoneAndGood(): Unit = {}

  private var whoStartedMe: Option[ActorRef] = None
  private val outstanding = new HashMap[Long, (ActorRef, Seq[IngestRecord])]
  private val outstandingNodes = new HashMap[ActorRef, Set[Long]].withDefaultValue(Set.empty)
  private var mapper: ShardMapper = ShardMapper.empty
  private var nextSeqId = 0L
  private val dataset = projection.datasetRef

  // *** Metrics ***
  private val kamonTags = Map("dataset" -> dataset.dataset, "version" -> version.toString)
  private val rowsIngested = Kamon.metrics.counter("source-rows-ingested", kamonTags)
  private val rowsReplayed = Kamon.metrics.counter("source-rows-replayed", kamonTags)
  private val unneededAcks = Kamon.metrics.counter("source-unneeded-acks", kamonTags)
  private val nodeHist     = Kamon.metrics.histogram("source-nodes-distributed", kamonTags)

  import context.dispatcher

  def start: Receive = LoggingReceive {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      whoStartedMe = Some(sender)
      logger.info(s"Starting, received initial shard map with ${newMap.numShards} shards")
      mapper = newMap
      self ! GetMoreRows
      logger.info(s" ==> Starting ingestion...")
      context.become(reading)
  }

  private def handleAck(seqNo: Long): Unit = {
    if (outstanding contains seqNo) {
      outstanding.remove(seqNo) foreach { case (nodeRef, _) =>
        outstandingNodes(nodeRef) = outstandingNodes(nodeRef) - seqNo
        if (outstandingNodes(nodeRef).isEmpty) outstandingNodes.remove(nodeRef)
      }
    } else { unneededAcks.increment }
  }

  def mapUpdate: Receive = LoggingReceive {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      logger.info(s"Received new partition map")
      mapper = newMap
  }

  def errorCatcher: Receive = LoggingReceive {
    case IngestionCommands.UnknownDataset =>
      whoStartedMe.foreach(_ ! IngestionErr(s"Ingestion actors shut down from ref $sender, check error logs"))

    case t: Throwable =>
      whoStartedMe.foreach(_ ! IngestionErr(s"Error from $sender, " + t.getMessage, Some(t)))

    case e: ErrorResponse =>
      whoStartedMe.foreach(_ ! IngestionErr(s"Error from $sender, " + e.toString))
  }

  def reading: Receive = (LoggingReceive {
    case GetMoreRows if batchIterator.hasNext => sendRows()
    case GetMoreRows =>
      if (outstanding.isEmpty)  { finish() }

    case IngestionCommands.Ack(lastSequenceNo) =>
      handleAck(lastSequenceNo)
      scheduledTask.foreach(_.cancel)
      if (outstanding.nonEmpty) schedule(ackTimeout, AckTimeout)
      // Keep ingestion going if below half of maxOutstanding items
      if (outstanding.size < (maxUnackedBatches / 2)) self ! GetMoreRows

    case IngestionCommands.Nack(seqNo) =>
      goToWaiting()

    case AckTimeout =>
      logger.warn(s" ==> (${self.path.name}) No Acks received for last $ackTimeout")
      goToWaiting()
  }) orElse mapUpdate orElse errorCatcher

  private def goToWaiting(): Unit = {
    logger.info(s" ==> (${self.path.name}) waiting: outstanding seqIds = ${outstanding.keys}")
    logger.info(s" ==>   outstanding nodes = ${outstandingNodes.keys}")
    schedule(waitingPeriod, CheckCanIngest)
    context.become(waiting)
  }

  def waitingAck: Receive = LoggingReceive {
    case IngestionCommands.Ack(lastSequenceNo) =>
      handleAck(lastSequenceNo)
      if (outstanding.isEmpty) {
        logger.info(s" ==> (${self.path.name}) reading, all unacked messages acked")
        self ! GetMoreRows
        scheduledTask.foreach(_.cancel)
        context.become(reading)
      }
  }

  def waiting: Receive = waitingAck orElse replay orElse mapUpdate orElse errorCatcher

  private def checkIngest(): Unit = {
    logger.debug(s"Checking if dataset $dataset can ingest...")
    outstandingNodes.keys.foreach(_ ! IngestionCommands.CheckCanIngest(dataset, version))
  }

  val replay: Receive = LoggingReceive {
    case GetMoreRows =>

    case IngestionCommands.ResumeIngest => checkIngest()
    case CheckCanIngest                 => checkIngest()

    case IngestionCommands.CanIngest(can) =>
      if (can) {
        val nodeRef = sender
        logger.debug(s" ==> (${self.path.name}) Replaying unacked messages with ids=${outstanding.keys}" +
                     s" from node $nodeRef")
        outstandingNodes(nodeRef).foreach { seqId =>
          val batch = outstanding(seqId)._2
          nodeRef ! IngestionCommands.IngestRows(dataset, version, batch, seqId)
          rowsReplayed.increment(batch.length)
        }
      }
      schedule(waitingPeriod, CheckCanIngest)
  }

  // Gets the next batch of data from the batchIterator, then determine if we can get more rows
  // or need to wait.
  def sendRows(): Unit = {
    val nextBatch = batchIterator.next

    // Convert rows to BinaryRecord first.  This takes care of handling any null inputs.
    logger.trace(s"  ==> BinaryRecord conversion for ${nextBatch.size} rows...")
    val binRecords = nextBatch.map { r =>
      try {
        r.copy(partition = BinaryRecord(projection.partKeyBinSchema, r.partition),
               data = BinaryRecord(projection.binSchema, r.data))
      } catch {
        case e: Exception =>
          logger.error(s"Could not convert source row $r to BinaryRecord", e)
          throw e
      }
    }

    // Now, compute a partition key hash for each row and group all the rows by the coordinator ref
    // returned from the shardMapper.  It is important this happens after BinaryRecord conversion,
    // because the partKeyFunc does not check for nulls.
    val rowsByNode = binRecords.groupBy { case IngestRecord(partition, _, _) =>
      mapper.partitionToShardNode(partition.hashCode).coord
    }
    nodeHist.record(rowsByNode.size)
    rowsByNode.foreach { case (nodeRef, records) =>
      logger.trace(s"  ==> ($nextSeqId) Sending ${records.size} records to node $nodeRef...")
      outstanding(nextSeqId) = (nodeRef, records)
      outstandingNodes(nodeRef) = outstandingNodes(nodeRef) + nextSeqId
      nodeRef ! IngestionCommands.IngestRows(dataset, version, records, nextSeqId)
      nextSeqId += 1
    }
    rowsIngested.increment(nextBatch.length)
    if (scheduledTask.isEmpty || scheduledTask.get.isCancelled) schedule(ackTimeout, AckTimeout)
    // Go get more rows
    if (outstanding.size < maxUnackedBatches) self ! GetMoreRows
  }

  protected var scheduledTask: Option[Cancellable] = None
  def schedule(delay: FiniteDuration, msg: Any): Unit = {
    scheduledTask.foreach(_.cancel)
    val task = context.system.scheduler.scheduleOnce(delay, self, msg)
    scheduledTask = Some(task)
  }

  def finish(): Unit = {
    logger.info(s"(${self.path.name}) Ingestion is all done")
    allDoneAndGood()
    whoStartedMe.foreach(_ ! AllDone)
    scheduledTask.foreach(_.cancel)
    // context.stop() stops remaining incoming messages, ensuring that we won't call finish() here multiple
    // times
    context.stop(self)
  }

  val receive = start
}

/**
 * A RowSource which is designed for direct ingestion into the MemStore.  No BinaryRecord, no Akka.
 */
trait DirectRowSource extends Actor with StrictLogging {
  import RowSource._

  def memStore: MemStore
  def projection: RichProjection

  def batchIterator: Iterator[Seq[IngestRecord]]

  private val rowsIngested = Kamon.metrics.counter("source-rows-ingested")

  def receive: Receive = LoggingReceive {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      val ref = projection.datasetRef
      logger.info(s"Starting direct ingestion for dataset $ref...")

      batchIterator.foreach { batch =>
        memStore.ingest(ref, batch)
        rowsIngested.increment(batch.length)
      }

      sender ! AllDone
      context.stop(self)
  }
}

class NoOpRowSource extends Actor with StrictLogging {
  def receive: Receive = {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      logger.info(s"Duh.....")
  }
}

/**
 * A RowSourceFactory to use when you want to just push and not pull any data.  It instantiates
 * NoOpRowSource which doesn't do anything.
 */
class NoOpSourceFactory extends RowSourceFactory {
  def create(config: Config, projection: RichProjection, memStore: MemStore): Props =
    Props(classOf[NoOpRowSource])
}