package filodb

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.metadata.{Column, Projection}
import filodb.core.reprojector.Reprojector
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkContext, TaskContext}
import org.velvia.filo.RowReader


package spark {

case class DatasetNotFound(dataset: String) extends Exception(s"Dataset $dataset not found")

case class DatasetAlreadyExists(dataset: String) extends Exception(s"Existing Dataset $dataset found")

// For each mismatch: the column name, DataFrame type, and existing column type
case class ColumnTypeMismatch(mismatches: Set[(String, DataType, Column.ColumnType)]) extends Exception

case class NoSegmentColumn(name: String) extends Exception(s"No segment columns specified for $name")

case class NoPrimaryColumn(name: String) extends Exception(s"No primary key columns specified for $name")

case class BadSchemaError(reason: String) extends Exception(reason)

case class UnsupportedColumnType(name: String, dataType: DataType)
  extends Exception(s"$dataType of column $name is unsupported")

}

package object spark {


  def configFromSpark(context: SparkContext): Config = {
    val conf = context.getConf
    val filoOverrides = conf.getAll.collect { case (k, v) if k.startsWith("spark.filodb") =>
      k.replace("spark.filodb.", "") -> v
    }
    import scala.collection.JavaConverters._
    ConfigFactory.parseMap(filoOverrides.toMap.asJava)
  }


  implicit class FiloContext(sqlContext: SQLContext) extends Serializable {

    import TypeConverters._

    def saveAsFiloDataset(df: DataFrame,
                          dataset: String,
                          parameters: Map[String, String] = Map.empty[String, String],
                          mode: SaveMode = SaveMode.Append): Unit = {
      val flushSize = parameters.getOrElse("flush-size", "10000").toInt

      val filoConfig = configFromSpark(sqlContext.sparkContext)
      Filo.init(filoConfig)
      val namesTypes = df.schema.map { f => f.name -> f.dataType }
      val ds = Filo.getDatasetObj(dataset).getOrElse(throw DatasetNotFound(dataset))
      val schema = ds.schema.map(col => col.name -> col)
      validateSchema(namesTypes, schema)

      val schemaMap = schema.toMap
      val dfOrderSchema = namesTypes.map { case (colName, x) =>
        schemaMap(colName)
      }
      // for each of the projections
      ds.projections.map { projection =>
        val tableWriter = TableWriter(flushSize, projection, dfOrderSchema)
        df.rdd.sparkContext.runJob(df.rdd, tableWriter.ingest _)
      }
    }

    case class TableWriter(flushSize: Int, projection: Projection, dfOrderSchema: Seq[Column]) {
      def ingest(taskContext: TaskContext, rowIter: Iterator[Row]):Unit = {
        implicit val executionContext = Filo.executionContext
        rowIter.grouped(flushSize).foreach { rows =>
          val flushes = Reprojector.project(projection,
            new RowIterator(rows.iterator)
            , Some(dfOrderSchema))
          flushes.foreach { flush =>
            Filo.parse(Filo.columnStore.flushToSegment(flush, false))(r => r)
          }
        }
      }
    }

    class RowIterator(iterator: Iterator[Row]) extends Iterator[RowReader] {
      val rowReader = new RddRowReader

      override def hasNext: Boolean = iterator.hasNext

      override def next(): RowReader = {
        rowReader.row = iterator.next()
        rowReader
      }
    }

    private def validateSchema(namesTypes: Seq[(String, DataType)], schema: Seq[(String, Column)]): Unit = {
      val dfNamesMap = namesTypes.toMap
      val schemaMap = schema.toMap

      val matchingCols = namesTypes.toMap.keySet.intersect(schema.toMap.keySet)
      // Type-check matching columns
      val matchingTypeErrs = matchingCols.collect {
        case colName: String if sqlTypeToColType(dfNamesMap(colName)) != schemaMap(colName).columnType =>
          (colName, dfNamesMap(colName), schemaMap(colName).columnType)
      }
      if (matchingTypeErrs.nonEmpty) throw ColumnTypeMismatch(matchingTypeErrs)
    }
  }

}
