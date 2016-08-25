package com.vngrs.etl.configs

import com.vngrs.etl.BaseSpec

/**
  * `CSV` to [[org.apache.spark.sql.Row]] Transformer Configs Specs
  */
class CsvToRowTransformerConfigsSpec extends BaseSpec {

  "A CSV to Row transformer configs specs" should "has default values" in {
    val config = CsvToRowTransformerConfigs()

    config.useHeader should equal (CsvToRowTransformerConfigs.defaultUseHeader)
    config.inferSchema should equal (CsvToRowTransformerConfigs.defaultInferSchema)
    config.dateFormat should equal (CsvToRowTransformerConfigs.defaultDateFormat)
    config.delimiter should equal (CsvToRowTransformerConfigs.defaultDelimiter)
    config.nullValue should equal (CsvToRowTransformerConfigs.defaultNullValue)
    config.escapeChar should equal (CsvToRowTransformerConfigs.defaultEscapeChar)
    config.quoteChar should equal (CsvToRowTransformerConfigs.defaultQuoteChar)
  }
}
