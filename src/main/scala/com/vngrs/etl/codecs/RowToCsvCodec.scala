package com.vngrs.etl.codecs

import java.util.Date

import com.vngrs.etl.configs.ToCsvCodecConfigs
import org.apache.spark.sql.Row

/**
  * [[org.apache.spark.sql.Row]] to `CSV` convertible format codec
  */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
final case class RowToCsvCodec() extends ToCsvCodec[Row] {

  /**
    * Converts [[org.apache.spark.sql.Row]] to `CSV` convertible format
    *
    * @param row [[org.apache.spark.sql.Row]]
    * @param conf Configurations
    * @return `CSV` convertible data
    */
  override def apply(row: Row, conf: ToCsvCodecConfigs): Seq[AnyRef] = {
    val seq = row.toSeq

    val formatVal = seq.map(_ => (v: Any) => Option(v).map {
      case d: Date => conf.dateFormatter.format(d).asInstanceOf[AnyRef]
      case a: Any => a.asInstanceOf[AnyRef]
    }.orNull)

    seq.zipWithIndex.map {
      case (fieldVal, i) => formatVal(i)(fieldVal)
    }
  }
}
