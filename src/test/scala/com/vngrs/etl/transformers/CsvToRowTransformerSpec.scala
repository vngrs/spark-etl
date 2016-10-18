package com.vngrs.etl.transformers

import java.util.{Calendar, TimeZone}

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.configs.CsvToRowTransformerConfigs
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * `CSV` formatted [[String]]s to [[org.apache.spark.sql.Row]]s Transformer Specs
  */
class CsvToRowTransformerSpec extends SparkSpec {

  "A CSV to Row transformer" should "read data with header" in {
    val csvData = Seq(
      """id,name,surname,birth_date""",
      """1,John,Doe,1980-01-01T00:00:00Z""",
      """2,Jane,Doe,1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = true))

    val rowRdd = transformer(csvRdd)

    val rowData = rowRdd.collect()

    rowData.length should equal (2)
    rowData.head.getString(1) should equal ("John")
    rowData.last.getString(2) should equal ("Doe")
  }

  it should "read data without header" in {
    val csvData = Seq(
      """1,John,Doe,1980-01-01T00:00:00Z""",
      """2,Jane,Doe,1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false))

    val rowRdd = transformer(csvRdd)

    val rowData = rowRdd.collect()

    rowData.length should equal (2)
    rowData.head.getString(1) should equal ("John")
    rowData.last.getString(2) should equal ("Doe")
  }

  it should "infer schema with header" in {
    val expectedSchema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("birth_date", DataTypes.TimestampType)
    ))

    val csvData = Seq(
      """id,name,surname,birth_date""",
      """1,John,Doe,1980-01-01T00:00:00Z""",
      """2,Jane,Doe,1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer()

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema should equal (expectedSchema)
  }

  it should "infer schema without header" in {
    val expectedSchema = StructType(Seq(
      StructField("C0", DataTypes.IntegerType),
      StructField("C1", DataTypes.StringType),
      StructField("C2", DataTypes.StringType),
      StructField("C3", DataTypes.TimestampType)
    ))

    val csvData = Seq(
      """1,John,Doe,1980-01-01T00:00:00Z""",
      """2,Jane,Doe,1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema should equal (expectedSchema)
  }

  it should "parse date time with timezone with default date time pattern" in {
    // to prevent this test to fail in different environments
    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val csvData = Seq(
      "2000-01-01T03:00:00+03:00"
    )

    val csvRdd = parallelize(csvData)

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0, 0)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().getTimestamp(0).getTime should equal (cal.getTimeInMillis)

    // to prevent this test to fail other tests because of timezone setup
    TimeZone.setDefault(defaultTimeZone)
  }

  it should "parse date time with custom date time pattern" in {
    val csvData = Seq(
      "2000/01/01 00:00Z"
    )

    val csvRdd = parallelize(csvData)

    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

    cal.clear()
    cal.set(2000, Calendar.JANUARY, 1, 0, 0)

    val transformer = CsvToRowTransformer(
      CsvToRowTransformerConfigs(useHeader = false, inferSchema = true, dateFormat = "yyyy/MM/dd HH:mmX")
    )

    val rowRdd = transformer(csvRdd)

    rowRdd.first().getTimestamp(0).getTime should equal (cal.getTimeInMillis)
  }

  it should "accepts tab as delimiter char" in {
    val csvData = Seq(
      """1	John	Doe	1980-01-01T00:00:00Z""",
      """2	Jane	Doe	1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      delimiter = '\t'
    ))

    val rowRdd = transformer(csvRdd)

    val rowData = rowRdd.collect()

    rowData.length should equal (2)
    rowData.head.getString(1) should equal ("John")
  }

  it should "accepts other delimiter chars" in {
    val csvData = Seq(
      """1|John|Doe|1980-01-01T00:00:00Z""",
      """2|Jane|Doe|1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      delimiter = '|'
    ))

    val rowRdd = transformer(csvRdd)

    val rowData = rowRdd.collect()

    rowData.length should equal (2)
    rowData.head.getString(1) should equal ("John")
  }

  it should "infer schema when custom delimiter char is given" in {
    val expectedSchema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("birth_date", DataTypes.TimestampType)
    ))

    val csvData = Seq(
      """id	name	surname	birth_date""",
      """1	John	Doe	1980-01-01T00:00:00Z""",
      """2	Jane	Doe	1985-01-01T00:00:00Z"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = true,
      inferSchema = true,
      delimiter = '\t'
    ))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema should equal (expectedSchema)
  }

  it should "handle null values in data" in {
    val csvData = Seq(
      "test,,test"
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false, inferSchema = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().isNullAt(1) should equal (true)
  }

  it should "handle custom null values in data" in {
    val csvData = Seq(
      "null,test"
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      nullValue = "null"
    ))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().isNullAt(0) should equal (true)
  }

  it should "not infer empty string as null when custom null value is given" in {
    val csvData = Seq(
      "null,"
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      nullValue = "null"
    ))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().isNullAt(1) should equal (false)
  }

  it should "infer column even first row of it is null" in {
    val csvData = Seq(
      ",test",
      "1,test"
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema.fields(0).dataType should equal (DataTypes.IntegerType)
  }

  it should "handle default escape character" in {
    val csvData = Seq(
      """test\,o\,test"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false, inferSchema = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema.fields.length should equal (1)
  }

  it should "handle custom escape character" in {
    val csvData = Seq(
      """test|,o|,test"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      escapeChar = '|'
    ))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().schema.fields.length should equal (1)
  }

  it should "handle default quote character" in {
    val csvData = Seq(
      """"test_quotes""""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(useHeader = false, inferSchema = false))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().getString(0) should equal ("test_quotes")
  }

  it should "handle custom quote character" in {
    val csvData = Seq(
      """|test_quotes|"""
    )

    val csvRdd = parallelize(csvData)

    val transformer = CsvToRowTransformer(CsvToRowTransformerConfigs(
      useHeader = false,
      inferSchema = false,
      quoteChar = '|'
    ))

    val rowRdd = transformer(csvRdd)

    rowRdd.first().getString(0) should equal ("test_quotes")
  }
}
