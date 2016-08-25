package com.vngrs.etl.transformers

import com.vngrs.etl.codecs.RowToCsvCodec
import com.vngrs.etl.configs.ToCsvTransformerConfigs
import com.vngrs.etl.utils.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Transforms [[org.apache.spark.sql.Row]]s to `CSV` formatted [[String]]s.
  *
  * @param config [[org.apache.spark.sql.Row]] to `CSV` transformers configs
  */
final case class RowToCsvTransformer
(
  config: ToCsvTransformerConfigs
) extends ToCsvTransformer[Row](RowToCsvCodec(), config) {

  /**
    * Converts header `CSV` convertible format
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    * @return Header in `CSV` formatted string
    */
  @inline
  override protected def createHeader(input: RDD[Row]): Array[AnyRef] = {
    input.schema()
      .orThrow("A schema is required when generating header in order to convert Row to CSV!")
      .fields.map(_.name)
  }
}

/**
  * [[com.vngrs.etl.transformers.RowToCsvTransformer]]'s companion object
  */
object RowToCsvTransformer {

  /**
    * Creates a [[com.vngrs.etl.transformers.RowToCsvTransformer]] with default configs
    *
    * @return [[com.vngrs.etl.transformers.RowToCsvTransformer]]
    * @see [[com.vngrs.etl.configs.ToCsvTransformerConfigs]]
    */
  def apply(): RowToCsvTransformer = RowToCsvTransformer(ToCsvTransformerConfigs())
}