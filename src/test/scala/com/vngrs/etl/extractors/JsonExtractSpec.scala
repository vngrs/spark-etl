package com.vngrs.etl.extractors

import com.vngrs.etl.SparkSpec
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * JSON Extract Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class JsonExtractSpec extends SparkSpec {

  "A json extractor" should "read a JSON file" in {
    val path = s"$rootFolder/parents.json"

    val extractor = JsonExtract(path)

    extractor.testCollect.length should equal (2)
  }

  it should "read multiple JSON files" in {
    val path = s"$rootFolder/parents.json,$rootFolder/childs.json"

    val extractor = JsonExtract(path)

    extractor.testCollect.length should equal (4)
  }

  it should "read multiple JSON files with wild cards" in {
    val path = s"$rootFolder/*.json"

    val extractor = JsonExtract(path)

    extractor.testCollect.length should equal (4)
  }

  it should "interpret JSON schema" in {
    val path = s"$rootFolder/parents.json"

    val extractor = JsonExtract(path)

    val interpretedSchema = extractor.testCollect.head.schema

    val expectedFields = Set(
      StructField("id", DataTypes.LongType),
      StructField("name", DataTypes.StringType),
      StructField("surname", DataTypes.StringType),
      StructField("age", DataTypes.LongType)
    )

    interpretedSchema.fields.toSet should contain theSameElementsAs expectedFields
  }

  it should "throw an exception when path does not exists" in {
    val path = s"$rootFolder/a_non_existed_file"

    val extractor = JsonExtract(path)

    intercept[Exception] {
      extractor.testCollect
    }
  }

  /** Root folder for test cases */
  private val rootFolder: String = getClass.getResource("/json_examples").getPath
}
