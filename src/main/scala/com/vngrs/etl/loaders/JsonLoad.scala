package com.vngrs.etl.loaders

import com.vngrs.etl.Load
import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.transformers.RowToJsonTransform
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Loads [[org.apache.spark.sql.Row]]s as JSON
  */
final case class JsonLoad(path: String) extends Load[Row] {

  /**
    * Saves content of the RDD in JSON Format (one object per line).
    *
    * @param input [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s.
    * @throws com.vngrs.etl.exceptions.NoSchemaException if given rows does not have a schema
    */
  @throws(classOf[NoSchemaException])
  override def apply(input: RDD[Row]): Unit = {
    val jsonRdd = RowToJsonTransform().apply(input)

    FileLoad(path).apply(jsonRdd)
  }
}
