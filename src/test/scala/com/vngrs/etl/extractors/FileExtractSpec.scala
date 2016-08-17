package com.vngrs.etl.extractors

import com.vngrs.etl.SparkSpec
import org.apache.hadoop.mapred.InvalidInputException

/**
  * File Extractor Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class FileExtractSpec extends SparkSpec {

  "A file extractor" should "read one file" in {
    val path = s"$rootFolder/file.txt"

    val extractor = FileExtract(path)

    extractor.testCollect should contain theSameElementsAs Set("1", "2", "3")
  }

  it should "read multiple files" in {
    val path = s"$rootFolder/*"

    val extractor = FileExtract(path)

    extractor.testCollect should contain theSameElementsAs Set("1", "2", "3", "4", "5", "6")
  }

  it should "throw an exception when path does not exists" in {
    val path = s"$rootFolder/a_non_existed_file"

    val extractor = FileExtract(path)

    intercept[InvalidInputException] {
      extractor.testCollect
    }
  }

  /** Root folder for test cases */
  private val rootFolder: String = getClass.getResource("/file_extract_spec").getPath
}
