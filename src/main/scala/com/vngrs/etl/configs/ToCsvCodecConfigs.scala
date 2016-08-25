package com.vngrs.etl.configs

import java.text.DateFormat

/**
  * To `CSV` convertible format codec configurations
  *
  * @param dateFormatter Date formatter
  */
final case class ToCsvCodecConfigs(dateFormatter: DateFormat) extends Configs
