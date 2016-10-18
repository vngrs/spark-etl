package com.vngrs.etl.codecs

import com.vngrs.etl.configs.ToCsvCodecConfigs

/**
  * Generic to `CSV` convertible format codec interface
  */
trait ToCsvCodec[A] extends Codec[A, Seq[AnyRef], ToCsvCodecConfigs]
