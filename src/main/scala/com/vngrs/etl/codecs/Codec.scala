package com.vngrs.etl.codecs

import com.vngrs.etl.configs.Configs

/**
  * Generic Converter Codec Interface
  *
  * @tparam A Type of the input
  * @tparam B Type of the output
  * @tparam CONF Type of the configurations
  */
trait Codec[A, B, CONF <: Configs] extends Serializable {

  def apply(a: A, conf: CONF): B
}
