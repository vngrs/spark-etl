package com.vngrs.etl.transformers

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.exceptions.NoSchemaException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * [[org.apache.spark.sql.Row]] to `JSON` formatted [[String]] Transformer Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class RowToJsonTransformSpec extends SparkSpec {

  "A Row to Json transformer" should "transform Row objects to Json formatted strings" in {
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

    val rowRdd = parallelize(data)

    val transformer = RowToJsonTransform()

    val jsonRdd = transformer(rowRdd)

    val jsonData = jsonRdd.collect().toSet

    val expectedData = Set(
      """{"id":1,"name":"John","surname":"Doe","age":35}""",
      """{"id":2,"name":"Jane","surname":"Doe","age":30}"""
    )

    jsonData.size should equal (2)
    jsonData should contain theSameElementsAs expectedData
  }

  it should "accept and handle empty RDDs" in {
    val rdd = emptyRDD[Row]

    val transformer = RowToJsonTransform()

    val jsonRdd = transformer(rdd)

    val jsonData = jsonRdd.collect()

    jsonData.length should equal (0)
    jsonData shouldBe Array[String]()
  }

  it should "throw exception when rows supplied without a schema" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    )

    val rdd = parallelize(data)

    val transformer = RowToJsonTransform()

    intercept[NoSchemaException] {
      transformer(rdd)
    }
  }

  it should "throw exception which contains meaningful info in the message when rows supplied without a schema" in {
    val data = Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    )

    val rdd = parallelize(data)

    val transformer = RowToJsonTransform()

    val thrown = intercept[NoSchemaException] {
      transformer(rdd)
    }

    thrown.getMessage.toLowerCase() should (include ("row") and include ("json"))
  }
}
