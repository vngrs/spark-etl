package com.vngrs.etl.loaders

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.utils.FileSystems
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.Random

/**
  * JSON Loader Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class JsonLoadSpec extends SparkSpec {

  "A json loader" should "load json data to files" in {
    val path = getClass.getResource("/").getPath + s"$randomString"

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

    val rdd = parallelize(data)

    val loader = JsonLoad(path)

    loader(rdd)

    new SQLContext(sc).read.json(path).count() should equal (2)

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "load json data to files (read check)" in {
    val path = getClass.getResource("/").getPath + s"$randomString"

    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    ))

    val data = Seq[Row](
      new GenericRowWithSchema(Array[Any](1L, "John", "Doe", 35L), schema)
    )

    val rdd = parallelize(data).coalesce(1)

    val loader = JsonLoad(path)

    loader(rdd)

    sc.textFile(path).collect().head should equal ("""{"id":1,"name":"John","surname":"Doe","age":35}""")

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  /** Generates a random Alpha Numeric String with a length of 20 **/
  private def randomString: String = Random.alphanumeric.take(20).mkString("")
}
