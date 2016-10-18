package com.vngrs.etl.configs

/**
  * `CSV` to [[org.apache.spark.sql.Row]] transformer configurations.
  * See companion object for default values.
  *
  * @param useHeader whether to use first record as header
  * @param inferSchema whether try to infer schema
  * @param dateFormat date format
  * @param delimiter delimiter
  * @param nullValue this value will be interpreted as `null`
  * @param quoteChar quote char
  * @param escapeChar escape char
  */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
final case class CsvToRowTransformerConfigs
(
  useHeader: Boolean = CsvToRowTransformerConfigs.defaultUseHeader,
  inferSchema: Boolean = CsvToRowTransformerConfigs.defaultInferSchema,
  dateFormat: String = CsvToRowTransformerConfigs.defaultDateFormat,
  delimiter: Char = CsvToRowTransformerConfigs.defaultDelimiter,
  nullValue: String = CsvToRowTransformerConfigs.defaultNullValue,
  quoteChar: Char = CsvToRowTransformerConfigs.defaultQuoteChar,
  escapeChar: Char = CsvToRowTransformerConfigs.defaultEscapeChar
) extends Configs

/**
  * [[com.vngrs.etl.configs.CsvToRowTransformerConfigs]] companion object.
  */
object CsvToRowTransformerConfigs {

  val defaultUseHeader = true

  val defaultInferSchema = true

  val defaultDateFormat = "yyyy-MM-dd'T'HH:mm:ssX"

  val defaultDelimiter = ','

  val defaultNullValue = ""

  val defaultQuoteChar = '\"'

  val defaultEscapeChar = '\\'
}

