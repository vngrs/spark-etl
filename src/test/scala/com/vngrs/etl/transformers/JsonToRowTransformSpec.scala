package com.vngrs.etl.transformers

import com.vngrs.etl.SparkSpec
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * `JSON` formatted [[String]] to [[org.apache.spark.sql.Row]] Transformer Specs
  */
class JsonToRowTransformSpec extends SparkSpec {

  "A Json to Row transformer" should "transform Json formatted strings to Row objects" in {
    val jsonData = Seq(
      """{ "id": 1, "name": "John", "surname": "Doe", "age": 35 }""",
      """{ "id": 2, "name": "Jane", "surname": "Doe", "age": 30 }"""
    )

    val jsonRdd = parallelize(jsonData)

    val transformer = JsonToRowTransform()

    val rowRdd = transformer(jsonRdd)

    val rows = rowRdd.collect()

    rows.length should equal (2)
    rows(0).getAs[String]("name") should equal("John")
    rows(1).getAs[String]("name") should equal("Jane")
  }

  it should "accept and handle empty RDDs" in {
    val rdd = emptyRDD[String]

    val transformer = JsonToRowTransform()

    val rowRdd = transformer(rdd)

    val rows = rowRdd.collect()

    rows.length should equal (0)
  }

  it should "interpret JSON schema" in {
    val jsonData = Seq(
      """{ "id": 1, "name": "John", "surname": "Doe", "age": 35 }""",
      """{ "id": 2, "name": "Jane", "surname": "Doe", "age": 30 }"""
    )

    val jsonRdd = parallelize(jsonData)

    val transformer = JsonToRowTransform()

    val rowRdd = transformer(jsonRdd)

    val interpretedSchema = rowRdd.collect().head.schema

    val expectedFields = Set(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    )

    interpretedSchema.fields.toSet should contain theSameElementsAs expectedFields
  }
}
