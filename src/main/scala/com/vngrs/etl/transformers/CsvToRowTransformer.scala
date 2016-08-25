package com.vngrs.etl.transformers

import com.databricks.spark.csv.CsvParser
import com.vngrs.etl.Transform
import com.vngrs.etl.configs.CsvToRowTransformerConfigs
import com.vngrs.etl.utils.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Transforms `CSV` formatted [[String]]s to [[org.apache.spark.sql.Row]]s.
  *
  * @param config `CSV` to [[org.apache.spark.sql.Row]] transformers configs
  */
final case class CsvToRowTransformer(config: CsvToRowTransformerConfigs) extends Transform[String, Row] {

  /**
    * Transforms `CSV` formatted [[String]]s to [[org.apache.spark.sql.Row]]s.
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    * @return Transformed [[org.apache.spark.rdd.RDD]]
    */
  override def apply(input: RDD[String]): RDD[Row] = {
    new CsvParser()
      .withUseHeader(config.useHeader)
      .withDelimiter(config.delimiter)
      .withInferSchema(config.inferSchema)
      .withDateFormat(config.dateFormat)
      .withQuoteChar(config.quoteChar)
      .withEscape(config.escapeChar)
      .withNullValue(config.nullValue)
      .withTreatEmptyValuesAsNulls(true)
      .csvRdd(input.sqlContext, input)
      .rdd
  }
}

/**
  * [[com.vngrs.etl.transformers.CsvToRowTransformer]]'s companion object
  */
object CsvToRowTransformer {

  /**
    * Creates a [[com.vngrs.etl.transformers.CsvToRowTransformer]] with default configs
    *
    * @return [[com.vngrs.etl.transformers.CsvToRowTransformer]]
    * @see [[com.vngrs.etl.configs.CsvToRowTransformerConfigs]]
    */
  def apply(): CsvToRowTransformer = new CsvToRowTransformer(CsvToRowTransformerConfigs())
}