package com.vngrs.etl.transformers

import java.text.SimpleDateFormat

import com.vngrs.etl.Transform
import com.vngrs.etl.codecs.ToCsvCodec
import com.vngrs.etl.configs.{ToCsvCodecConfigs, ToCsvTransformerConfigs}
import org.apache.commons.csv.CSVFormat
import org.apache.spark.rdd.RDD

/**
  * Transforms [[A]]s to `CSV` formatted [[String]]s with help of given `toCsvCodec`.
  *
  * @param toCsvCodec [[A]] to `CSV` codec
  * @param config To `CSV` transformer configurations
  * @tparam A Type of the data ([[org.apache.spark.rdd.RDD]]).
  */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Var"))
abstract class ToCsvTransformer[A]
(
  toCsvCodec: ToCsvCodec[A],
  config: ToCsvTransformerConfigs
) extends Transform[A, String] with Serializable {

  /**
    * Transforms [[A]]s to `CSV` formatted [[String]]s with help of given `toCsvCodec`.
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    * @return Transformed [[org.apache.spark.rdd.RDD]]
    */
  override def apply(input: RDD[A]): RDD[String] = {

    val header =
      if (config.generateHeader) {
        createHeader(input)
      } else {
        Array[AnyRef]()
      }

    input.mapPartitions[String](iterator =>
      new Iterator[String] {

        private var firstRowAndShouldGenerateHeader = config.generateHeader

        private val codecConf = ToCsvCodecConfigs(new SimpleDateFormat(config.dateFormat))

        private val csvFormat = createCsvFormat()

        override def hasNext: Boolean = iterator.hasNext

        override def next: String = {
          if (firstRowAndShouldGenerateHeader) {
            firstRowAndShouldGenerateHeader = false
            csvFormat.format(header: _*)
          } else {
            csvFormat.format(toCsvCodec(iterator.next(), codecConf): _*)
          }
        }
      }
    )
  }

  /**
    * Creates [[org.apache.commons.csv.CSVFormat]]
    *
    * @return [[org.apache.commons.csv.CSVFormat]]
    */
  @inline
  private def createCsvFormat(): CSVFormat = {
    CSVFormat.DEFAULT
      .withDelimiter(config.delimiter)
      .withQuote(config.quoteChar)
      .withEscape(config.escapeChar)
      .withNullString(config.nullValue)
      .withQuoteMode(config.quoteMode)
  }

  /**
    * Converts header `CSV` convertible format
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    * @return Header in `CSV` formatted string
    */
  @inline
  protected def createHeader(input: RDD[A]): Array[AnyRef]
}
