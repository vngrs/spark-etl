package com.vngrs.etl.transformers

import com.vngrs.etl.Transform
import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.utils.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Transforms given `input` with given `query` and `tableName`.
  *
  * @param query SQL Query
  * @param tableName Temporary table name
  * @throws IllegalArgumentException if given query does not contains given table name
  */
@throws(classOf[IllegalArgumentException])
final case class SqlTransform(query: String, tableName: String) extends Transform[Row, Row] {

  // checking validity of the query
  require(query.contains(tableName), s"Given query ($query) must contain table name - $tableName!")

  /**
    * Transforms given `input` with given `query` and `tableName`.
    *
    * @param input [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    * @return Transformed [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    */
  override def apply(input: RDD[Row]): RDD[Row] = {
    input.schema().fold(
      schema => {
        val sqlCtx = new SQLContext(input.sparkContext)

        sqlCtx.createDataFrame(input, schema).registerTempTable(tableName)

        val result = sqlCtx.sql(query)

        sqlCtx.dropTempTable(tableName)

        result.rdd
      },
      throw new NoSchemaException("Schema is required to apply sql transforms on rows!"),
      input
    )
  }
}

/**
  * Companion object for [[com.vngrs.etl.transformers.SqlTransform]]
  */
object SqlTransform {

  /**
    * Default temporary table name for [[com.vngrs.etl.transformers.SqlTransform]]'s queries
    */
  val defaultTableName = "setl_table"

  /**
    * Transforms given `input` with given `query` on default table name - `setl_table`.
    *
    * @param query SQL Query
    * @return Transformed [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    * @throws IllegalArgumentException if given query does not contains `setl_table`.
    */
  @throws(classOf[IllegalArgumentException])
  def apply(query: String): SqlTransform = new SqlTransform(query, defaultTableName)
}
