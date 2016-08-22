package com.vngrs.etl.utils

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.utils.rdd._
import com.vngrs.etl.utils.rdd.SchemaResult.{EmptyRdd, Found, NotFound}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Enriched RDDs Specs in [[com.vngrs.etl.utils.rdd]]
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Var"))
class EnrichedRddSpec extends SparkSpec {

  "A Rich RDD" should "check whether it is non empty or not for empty RDDs" in {
    val rdd = emptyRDD[String]

    rdd.nonEmpty() should equal (false)
  }

  it should "check whether it is non empty or not for non empty RDDs" in {
    val rdd = parallelize(Seq(1, 2, 3))

    rdd.nonEmpty() should equal (true)
  }

  it should "check whether it is non empty or not for non empty RDDs even it is partitioned to many" in {
    val rdd = parallelize(Seq(1, 2, 3)).repartition(30)

    rdd.nonEmpty() should equal (true)
  }

  "A Rich Row RDD" should "get schema wrapped with Found when there is one" in {
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = parallelize(Seq[Row](new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema)))

    data.schema() should equal (Found(schema))
  }

  it should "get NotFound when there is none" in {
    val data = parallelize(Seq[Row](new GenericRow(Array[Any](1L, "John", "Doe", 35L))))

    data.schema() should equal (NotFound)
  }

  it should "get EmptyRdd when rdd is empty" in {
    val data = emptyRDD[Row]

    data.schema() should equal (EmptyRdd)
  }

  it should "get schema as optional" in {
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = parallelize(Seq[Row](new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema)))

    data.schema().toOption shouldBe Some(schema)
  }

  it should "handle empty RDDs" in {
    val data = emptyRDD[Row]

    data.schema().toOption shouldBe None
  }

  it should "throw if given rows does not have schema info" in {
    val data = parallelize(Seq[Row](new GenericRow(Array[Any](1L, "John", "Doe", 35L))))

    intercept[NoSchemaException] {
      data.schema().orThrow("Some Message")
    }
  }

  it should "throw exception when an empty RDD is supplied" in {
    val data = emptyRDD[Row]

    intercept[NoSchemaException] {
      data.schema().orThrow("Some Message")
    }
  }

  it should "throw exception with given message when no schema is found" in {
    val data = emptyRDD[Row]

    val thrown = intercept[NoSchemaException] {
      data.schema().orThrow("No Schema Exception Test Message")
    }

    thrown.getMessage should equal ("No Schema Exception Test Message")
  }

  it should "should trigger on found event when getting schema when there is one" in {
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = parallelize(Seq[Row](new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema)))

    var triggered = false

    data.schema().fold(
      _ => triggered = true,
      throw new Exception("onNotFound should not have been triggered"),
      throw new Exception("onEmptyRdd should not have been triggered")
    )

    triggered should equal (true)
  }

  it should "should trigger on not found event when getting schema when there is none" in {
    val data = parallelize(Seq[Row](new GenericRow(Array[Any](1L, "John", "Doe", 35L))))

    var triggered = false

    data.schema().fold(
      _ => throw new Exception("onFound should not have been triggered"),
      triggered = true,
      throw new Exception("onEmptyRdd should not have been triggered")
    )

    triggered should equal (true)
  }

  it should "should trigger on empty rdd event when getting schema when rdd is empty" in {
    val data = emptyRDD[Row]

    var triggered = false

    data.schema().fold(
      _ => throw new Exception("onFound should not have been triggered"),
      throw new Exception("onNotFound should not have been triggered"),
      triggered = true
    )

    triggered should equal (true)
  }
}
