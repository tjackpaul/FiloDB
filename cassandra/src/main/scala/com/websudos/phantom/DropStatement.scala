package com.websudos.phantom

import com.websudos.phantom.builder.query.{CQLQuery, ExecutableStatement, QueryOptions}

case class DropStatement(keyspace: String, tableName: String, ifExists: Boolean = false) extends ExecutableStatement {
  val ifExistsStr = if (ifExists) "IF EXISTS " else ""

  override def qb: CQLQuery = CQLQuery(s"DROP TABLE $ifExistsStr $keyspace.$tableName")

  override def options: QueryOptions = QueryOptions.empty
}
