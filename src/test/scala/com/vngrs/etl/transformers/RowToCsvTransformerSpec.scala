package com.vngrs.etl.transformers

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.codecs.RowToCsvCodec
import com.vngrs.etl.configs.{ToCsvCodecConfigs, ToCsvTransformerConfigs}
import com.vngrs.etl.exceptions.NoSchemaException
import org.apache.commons.csv.QuoteMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * [[org.apache.spark.sql.Row]]s to `CSV` formatted [[String]]s Transformer Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Null"))
class RowToCsvTransformerSpec extends SparkSpec {

  "A Row to CSV transformer" should "convert rows to csv formatted strings" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer()

    transformer(dataRdd).collect() should contain theSameElementsAs Array("1,John,Doe,35", "2,Jane,Doe,30")

  }

  it should "handle empty rows" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]()),
      new GenericRow(Array[Any]())
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer()

    transformer(dataRdd).collect() should contain theSameElementsAs Array("", "")
  }

  it should "generate header if requested" in {
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = Seq[Row](
      new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema),
      new GenericRowWithSchema(Array[Any](2L, "Jane", "Doe", 30L), schema)
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(generateHeader = true))

    transformer(dataRdd).collect().head should equal ("id,name,surname,age")
  }

  it should "generate header with custom delimiter if requested" in {
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = Seq[Row](
      new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema),
      new GenericRowWithSchema(Array[Any](2L, "Jane", "Doe", 30L), schema)
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(generateHeader = true, delimiter = '\t'))

    transformer(dataRdd).collect().head should equal ("id	name	surname	age")
  }

  it should "throw an exception if header generation requested and there is no schema in the row" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(generateHeader = true))

    intercept[NoSchemaException] {
      transformer(dataRdd).collect()
    }
  }

  it should "convert date objects to string" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val data = Seq[Row](
      new GenericRow(Array[Any](cal.getTime))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer()

    transformer(dataRdd).collect().head should equal ("2000-01-01T00:00:00Z")

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "convert date objects to string with custom date format" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val data = Seq[Row](
      new GenericRow(Array[Any](cal.getTime))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(dateFormat = "yyyy/MM/dd HH:mm:ss"))

    transformer(dataRdd).collect().head should equal ("2000/01/01 00:00:00")

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "use custom delimiter supplied with config" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(delimiter = '\t'))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("1	John	Doe	35", "2	Jane	Doe	30")
  }

  it should "put default null value string for null values" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](null, null)),
      new GenericRow(Array[Any](null, null))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer()

    transformer(dataRdd).collect() should contain theSameElementsAs Array(",", ",")
  }

  it should "put custom null value string for null values" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](null, null)),
      new GenericRow(Array[Any](null, null))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(nullValue = "null_value"))

    val expectedData = Array("null_value,null_value", "null_value,null_value")

    transformer(dataRdd).collect() should contain theSameElementsAs expectedData
  }

  it should "put default escape char when comma found in string values (when quote mode is none)" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", ",data")),
      new GenericRow(Array[Any]("test,", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(quoteMode = QuoteMode.NONE))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("test,\\,data", "test\\,,data")
  }

  it should "put custom escape char when comma found in string values (when quote mode is none)" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", ",data")),
      new GenericRow(Array[Any]("test,", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(escapeChar = '|', quoteMode = QuoteMode.NONE))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("test,|,data", "test|,,data")
  }

  it should "put default quotes when special char is in data" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", "!data")),
      new GenericRow(Array[Any]("!test", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer()

    transformer(dataRdd).collect() should contain theSameElementsAs Array("test,\"!data\"", "\"!test\",data")
  }

  it should "put custom quotes when special char is in data" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", "!data")),
      new GenericRow(Array[Any]("!test", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(quoteChar = '|'))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("test,|!data|", "|!test|,data")
  }

  it should "put no quotes when quote mode is none" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", "!data")),
      new GenericRow(Array[Any]("!test", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(quoteMode = QuoteMode.NONE))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("test,!data", "!test,data")
  }

  it should "put always quotes when quote mode is all" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", "data")),
      new GenericRow(Array[Any]("test", "data"))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(quoteMode = QuoteMode.ALL))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("\"test\",\"data\"", "\"test\",\"data\"")
  }

  it should "put quotes to non numeric when quote mode is non numeric" in {
    val data = Seq[Row](
      new GenericRow(Array[Any]("test", 1)),
      new GenericRow(Array[Any]("test", 1))
    )

    val dataRdd = parallelize(data)

    val transformer = RowToCsvTransformer(ToCsvTransformerConfigs(quoteMode = QuoteMode.NON_NUMERIC))

    transformer(dataRdd).collect() should contain theSameElementsAs Array("\"test\",1", "\"test\",1")
  }
}
