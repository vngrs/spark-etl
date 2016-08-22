package com.vngrs.etl.transformers

import com.vngrs.etl.Transform
import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.utils.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Transform [[org.apache.spark.sql.Row]]s to `JSON` formatted [[String]]s.
  */
final case class RowToJsonTransform() extends Transform[Row, String] {

  /**
    * No schema found exception message
    */
  private val noSchemaMsg = "A Schema expected to be found in Row in order to convert Row to JSON but none is found!"

  /**
    * Transform [[org.apache.spark.sql.Row]]s to `JSON` formatted [[String]]s.
    *
    * @param input [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    * @return Transformed [[org.apache.spark.rdd.RDD]] of `JSON` [[String]]s
    * @throws com.vngrs.etl.exceptions.NoSchemaException if given rows does not have a schema
    */
  @throws(classOf[NoSchemaException])
  override def apply(input: RDD[Row]): RDD[String] = {
    input.schema().fold(
      schema => {
        val sqlCtx = new SQLContext(input.sparkContext)
        sqlCtx.createDataFrame(input, schema).toJSON
      },
      throw new NoSchemaException(noSchemaMsg),
      input.sparkContext.emptyRDD[String]
    )
  }
}
