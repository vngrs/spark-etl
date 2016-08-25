package com.vngrs.etl.configs

import com.vngrs.etl.BaseSpec

/**
  * To `CSV` Transformer Configs Specs
  */
class ToCsvTransformerConfigsSpec extends BaseSpec {

  "A to CSV transformer configs specs" should "has default values" in {
    val config = ToCsvTransformerConfigs()

    config.generateHeader should equal (ToCsvTransformerConfigs.defaultGenerateHeader)
    config.dateFormat should equal (ToCsvTransformerConfigs.defaultDateFormat)
    config.delimiter should equal (ToCsvTransformerConfigs.defaultDelimiter)
    config.nullValue should equal (ToCsvTransformerConfigs.defaultNullValue)
    config.escapeChar should equal (ToCsvTransformerConfigs.defaultEscapeChar)
    config.quoteChar should equal (ToCsvTransformerConfigs.defaultQuoteChar)
    config.quoteMode should equal (ToCsvTransformerConfigs.defaultQuoteMode)
  }
}
