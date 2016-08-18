package com.vngrs.etl.transformers

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.matchers.SchemaMatchers._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * SQL Transformer Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SqlTransformSpec extends SparkSpec {

  "An SQL Transformer" should "select specified columns" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT name, surname FROM ${SqlTransform.defaultTableName}")

    val transformedRdd = transformer(data)

    transformedRdd should hasSameFieldNamesWith (Set("name", "surname"))
  }

  it should "transforms columns" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT CONCAT(name, ' ', surname) FROM ${SqlTransform.defaultTableName}")

    val transformedRdd = transformer(data)

    transformedRdd.collect().map(_.getString(0)).toSet should contain theSameElementsAs Set("John Doe", "Jane Doe")
  }

  it should "rename column name" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT surname as lastName FROM ${SqlTransform.defaultTableName}")

    val transformedRdd = transformer(data)

    transformedRdd should hasSameFieldNamesWith (Set("lastName"))
  }

  it should "filter rows" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT * FROM ${SqlTransform.defaultTableName} WHERE age > 30")

    val transformedRdd = transformer(data)

    transformedRdd.collect().length should equal (1)
  }

  it should "count rows" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT COUNT(*) FROM ${SqlTransform.defaultTableName}")

    val transformedRdd = transformer(data)

    val transformedData = transformedRdd.map(_.getLong(0)).collect()

    transformedData.length should equal (1)
    transformedData.head should equal (2)
  }

  it should "filter and count rows" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT COUNT(1) FROM ${SqlTransform.defaultTableName} WHERE age > 30")

    val transformedRdd = transformer(data)

    val transformedData = transformedRdd.map(_.getLong(0)).collect()

    transformedData.length should equal (1)
    transformedData.head should equal (1)
  }

  it should "accept empty RDDs" in {
    val data = emptyRDD[Row]

    val transformer = SqlTransform(s"SELECT * FROM ${SqlTransform.defaultTableName}")

    val transformedRdd = transformer(data)

    transformedRdd.count() should equal (0)
  }

  it should "accept custom temporary table name" in {
    val data = generateRowData()

    val transformer = SqlTransform(s"SELECT * FROM test_temp_table", "test_temp_table")

    val transformedRdd = transformer(data)

    transformedRdd.count() should equal (2)
  }

  it should "throw no schema found exception if given rows does not have any schema" in {
    val data = parallelize(Seq[Row](
      new GenericRow(Array[Any](1L, "John", "Doe", 35L)),
      new GenericRow(Array[Any](2L, "Jane", "Doe", 30L))
    ))

    val transformer = SqlTransform(s"SELECT * FROM ${SqlTransform.defaultTableName}")

    intercept[NoSchemaException] {
      transformer(data)
    }
  }

  it should "throw an exception if given query does not contain default table name" in {
    intercept[IllegalArgumentException]{
      SqlTransform(s"SELECT * FROM NOPE_I_AM_NOT_THE_ONE!")
    }
  }

  it should "throw an exception if given query does not contain given custom table name" in {
    intercept[IllegalArgumentException]{
      SqlTransform(s"SELECT * FROM NOPE_I_AM_NOT_THE_ONE!", "test_temp_table")
    }
  }

  private def generateRowData(): RDD[Row] = {
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

    parallelize(data)
  }
}
