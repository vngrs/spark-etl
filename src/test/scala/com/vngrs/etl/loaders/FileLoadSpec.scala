package com.vngrs.etl.loaders

import com.vngrs.etl.SparkSpec
import com.vngrs.etl.utils.FileSystems

import scala.util.Random

/**
  * File loader spec
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class FileLoadSpec extends SparkSpec {

  "A file loader" should "write to file" in {
    val path = s"$rootFolder/$randomString"

    val loader = FileLoad(path)

    val data = parallelize(Seq("1", "2", "3", "4", "5", "6"))

    loader(data)

    FileSystems.exists(sc, path) should equal (true)

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "write to file (read check)" in {
    val path = s"$rootFolder/$randomString"

    val loader = FileLoad(path)

    val data = parallelize(Seq("test data")).coalesce(1)

    loader(data)

    sc.textFile(path).collect().head should equal ("test data")

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  /** Root folder for test cases */
  private val rootFolder: String = getClass.getResource("/").getPath + "/file_load_spec"

  /** Generates a random Alpha Numeric String with a length of 20 */
  private def randomString: String = Random.alphanumeric.take(20).mkString("")
}
