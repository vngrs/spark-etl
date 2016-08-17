package com.vngrs.etl.transformers

import com.vngrs.etl.Transform
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Transform `JSON` formatted [[String]]s to [[org.apache.spark.sql.Row]]s.
  */
final case class JsonToRowTransform() extends Transform[String, Row] {

  /**
    * Transform `JSON` formatted [[String]]s to [[org.apache.spark.sql.Row]]s.
    *
    * @param input [[org.apache.spark.rdd.RDD]] of `JSON` formatted [[String]]s.
    * @return Transformed [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s.
    */
  override def apply(input: RDD[String]): RDD[Row] = {
    val sqlCtx = new SQLContext(input.sparkContext)

    sqlCtx.read.json(input).rdd
  }
}
