package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.core.Messages.Response
import filodb.core.store.ColumnStore

import scala.concurrent.{ExecutionContext, Future}

class CassandraColumnStore(val keySpace: KeySpace, val session: Session)(implicit val ec: ExecutionContext)
  extends ColumnStore
  with CassandraChunkStore
  with CassandraSummaryStore
  with CassandraQueryApi {

  override def summaryTable: SummaryTable = new SummaryTable(keySpace, session)

  def initialize: Future[Seq[Response]] = {
    flow.warn(s"Initializing column store")
    Future sequence Seq(summaryTable.initialize())
  }

  def clearAll: Future[Seq[Response]] = {
    flow.warn(s"Removing all data")
    Future sequence Seq(summaryTable.clearAll())
  }

}

