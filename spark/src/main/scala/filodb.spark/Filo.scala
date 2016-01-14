package filodb.spark

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import filodb.cassandra.FiloCassandraConnector
import filodb.core.metadata.Column
import filodb.core.store.{Dataset, ProjectionInfo}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object Filo extends Serializable {

  lazy val system = ActorSystem("filo-spark")

  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  lazy val connector = new FiloCassandraConnector(filoConfig.getConfig("cassandra"))
  lazy val metaStore = connector.metaStore
  lazy val columnStore = connector.columnStore

  // scalastyle:off
  var filoConfig: Config = null

  def init(config: Config) = {
    if (filoConfig == null) {
      filoConfig = config
    }
  }

  // scalastyle:on

  def getDatasetObj(dataset: String): Option[Dataset] =
    Filo.parse(metaStore.getDataset(dataset)) { ds => ds }

  def deleteDataset(datasetObj: Dataset): Unit = {
    datasetObj.projections.foreach { projection =>
      Filo.parse(columnStore.deleteProjectionData(projection)) { r => r }
      Filo.parse(
        metaStore.deleteDataset(datasetObj.name, Some(projection.id))
      ) { ds => ds }
    }
  }

  def addProjection(projectionInfo: ProjectionInfo): Boolean = {
    Filo.parse(metaStore.addProjection(projectionInfo)) { ds => ds }
  }


  def getSchema(dataset: String, version: Int): Seq[Column] =
    Filo.parse(metaStore.getSchema(dataset)) { schema => schema }

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 50000.seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  def newActor(props: Props): ActorRef = system.actorOf(props)

}
