package com.vngrs.etl.configs

import org.apache.commons.csv.QuoteMode

/**
  * To `CSV` transformer configurations.
  * See companion object for default values.
  *
  * @param generateHeader whether to generate header
  * @param dateFormat date format
  * @param delimiter delimiter
  * @param nullValue this value will be interpreted as `null`
  * @param escapeChar escape char
  * @param quoteChar quote char
  * @param quoteMode quote mode
  */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
final case class ToCsvTransformerConfigs
(
  generateHeader: Boolean = ToCsvTransformerConfigs.defaultGenerateHeader,
  dateFormat: String = ToCsvTransformerConfigs.defaultDateFormat,
  delimiter: Char = ToCsvTransformerConfigs.defaultDelimiter,
  nullValue: String = ToCsvTransformerConfigs.defaultNullValue,
  escapeChar: Char = ToCsvTransformerConfigs.defaultEscapeChar,
  quoteChar: Char = ToCsvTransformerConfigs.defaultQuoteChar,
  quoteMode: QuoteMode = ToCsvTransformerConfigs.defaultQuoteMode
) extends Configs

/**
  * [[com.vngrs.etl.configs.ToCsvTransformerConfigs]] companion object.
  */
object ToCsvTransformerConfigs {

  val defaultGenerateHeader = false

  val defaultDateFormat = "yyyy-MM-dd'T'HH:mm:ssX"

  val defaultDelimiter = ','

  val defaultNullValue = ""

  val defaultEscapeChar = '\\'

  val defaultQuoteChar = '\"'

  val defaultQuoteMode = QuoteMode.MINIMAL
}
