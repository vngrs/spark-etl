package com.vngrs.etl.extractors

import com.vngrs.etl.Extract
import com.vngrs.etl.transformers.JsonToRowTransform
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Extracts a JSON file(s).
  *
  * @param path File path(s).
  *   In order to supply multiple files, you can use wildcards or give multiple paths separated with comma.
  */
final case class JsonExtract(path: String) extends Extract[Row] {

  /**
    * Extracts a JSON file (one object per line) and returns the result as a
    * [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]].
    *
    * @param sc [[org.apache.spark.SparkContext]]
    * @return Extracted [[org.apache.spark.rdd.RDD]]
    */
  override def apply(sc: SparkContext): RDD[Row] = {
    val data = FileExtract(path).apply(sc)

    JsonToRowTransform().apply(data)
  }
}
