package com.vngrs.etl.utils

import java.io.IOException

import com.vngrs.etl.SparkSpec

import scala.util.Random

/**
  * File System Utility Specs
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class FileSystemsSpec extends SparkSpec {

  //region Path Existence Checker Spec

  "A path existence checker" should "check and validate a file is existed" in {
    val path = s"$rootFolder/a_file.txt"

    FileSystems.exists(sc, path) should equal (true)
  }

  it should "check and validate that another file does not exist" in {
    val path = s"$rootFolder/non_existed_file.txt"

    FileSystems.exists(sc, path) should equal (false)
  }

  it should "throw an exception if given path is invalid" in {
    val path = "an_invalid_path://path_to_file"

    intercept[IllegalArgumentException] {
      FileSystems.exists(sc, path)
    }
  }

  it should "throw an exception when path is an empty string" in {
    val path = ""

    intercept[IllegalArgumentException] {
      FileSystems.exists(sc, path)
    }
  }

  //endregion Path Existence Checker Spec

  //region File Reader Spec

  "A file reader" should "read non empty file" in {
    val path = s"$rootFolder/a_file.txt"

    FileSystems.read(sc, path) should equal ("Content of a file.")
  }

  it should "read empty file" in {
    val path = s"$rootFolder/an_empty_file.txt"

    FileSystems.read(sc, path) should equal ("")
  }

  it should "read a multiline file" in {
    val path = s"$rootFolder/a_multiline_file.txt"

    FileSystems.read(sc, path) should equal ("This\nis\na\nMULTILINE\nfile.")
  }

  it should "throw an exception when file does not exists" in {
    val path = s"$rootFolder/non_existed_file.txt"

    intercept[IOException] {
      FileSystems.read(sc, path)
    }
  }

  it should "throw an exception if given path is invalid" in {
    val path = "an_invalid_path://path_to_file"

    intercept[IllegalArgumentException] {
      FileSystems.read(sc, path)
    }
  }

  it should "throw an exception when path is an empty string" in {
    val path = ""

    intercept[IllegalArgumentException] {
      FileSystems.read(sc, path)
    }
  }

  //endregion File Reader Spec

  //region File Reader with Default Spec

  "A file reader with default value" should "read file" in {
    val path = s"$rootFolder/a_file.txt"

    val default = ""

    FileSystems.readWithDefault(sc, path, default) should equal ("Content of a file.")
  }

  it should "write and return given default value when file does not exists" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    val default = s"another file content with random string => $randomString."

    FileSystems.readWithDefault(sc, path, default) should equal (default)

    //cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "throw an exception if given path is invalid" in {
    val path = "an_invalid_path://path_to_file"

    val default = ""

    intercept[IllegalArgumentException] {
      FileSystems.readWithDefault(sc, path, default)
    }
  }

  it should "throw an exception when path is an empty string" in {
    val path = ""

    val default = ""

    intercept[IllegalArgumentException] {
      FileSystems.readWithDefault(sc, path, default)
    }
  }

  //endregion File Reader with Default Spec

  //region File Writer Spec

  "A file writer" should "write non empty file" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    val data = s"Data $randomString"

    FileSystems.write(sc, path, data)

    FileSystems.read(sc, path) should equal (data)

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "write empty file" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    val data = ""

    FileSystems.write(sc, path, data)

    FileSystems.read(sc, path) should equal (data)

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "overwrite file" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    val firstData = s"Data $randomString"
    FileSystems.write(sc, path, firstData)

    val secondData = s"Updated Data $randomString"
    FileSystems.write(sc, path, secondData)

    FileSystems.read(sc, path) should equal (secondData)

    // cleanup
    try { FileSystems.delete(sc, path) }
  }

  it should "create directories when writing a file" in {
    val dirPath = s"$rootFolder/dir_one/dir_two/dir_three"
    val filePath = s"$dirPath/file.txt"

    val data = s"data $randomString"

    FileSystems.write(sc, filePath, data)

    FileSystems.exists(sc, dirPath) should equal (true)
    FileSystems.exists(sc, filePath) should equal (true)

    FileSystems.read(sc, filePath) should equal (data)

    // cleanup
    try { FileSystems.delete(sc, s"$rootFolder/dir_one") }
  }

  it should "throw an exception when overwriting disabled" in {
    val path = s"$rootFolder/a_file.txt"

    val data = "Updated Data"

    intercept[IOException] {
      FileSystems.write(sc, path, data, overwrite = false)
    }
  }

  //endregion File Writer Spec

  //region Path Deleter Spec

  "A path deleter" should "delete a file" in {
    val path = s"$rootFolder/to_be_deleted.txt"

    // creating file
    FileSystems.write(sc, path, "to be deleted.")

    FileSystems.delete(sc, path) should equal (true)

    // additional check
    FileSystems.exists(sc, path) should equal (false)
  }

  it should "delete a folder recursively" in {
    val path = s"$rootFolder/to_be_deleted_dir/to_be_deleted.txt"

    // creating file
    FileSystems.write(sc, path, "to be deleted.")

    FileSystems.delete(sc, path) should equal (true)

    // additional check
    FileSystems.exists(sc, path) should equal (false)
  }

  it should "throw an exception when folder is not empty and recursive is disabled" in {
    val dirPath = s"$rootFolder/not_to_be_deleted_dir"
    val filePath = s"$dirPath/not_to_be_deleted.txt"

    // creating file
    FileSystems.write(sc, filePath, "not to be deleted.")

    intercept[IOException] {
      FileSystems.delete(sc, dirPath, recursive = false)
    }

    // additional check
    FileSystems.exists(sc, dirPath) should equal(true)

    // cleanup
    try { FileSystems.delete(sc, dirPath) }
  }

  it should "return false when file does not exists" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    FileSystems.delete(sc, path) should equal (false)
  }

  it should "return false when file does not exists when recursive is disabled" in {
    val path = s"$rootFolder/non_existed_file_$randomString.txt"

    FileSystems.delete(sc, path, recursive = false) should equal (false)
  }

  it should "throw an exception if given path is invalid" in {
    val path = "an_invalid_path://path_to_file"

    intercept[IllegalArgumentException] {
      FileSystems.delete(sc, path)
    }
  }

  //endregion Path Deleter Spec

  //region Path Rename Method Spec

  "A path rename method" should "rename file" in {
    val srcPath = s"$rootFolder/src_path_$randomString"
    val dstPath = s"$rootFolder/dst_path_$randomString"

    val data = s"data $randomString"

    // create a file
    FileSystems.write(sc, srcPath, data)

    FileSystems.rename(sc, srcPath, dstPath) should equal (true)

    // existence check
    FileSystems.exists(sc, srcPath) should equal (false)
    FileSystems.exists(sc, dstPath) should equal (true)

    // content check
    FileSystems.read(sc, dstPath) should equal (data)


    // cleanup
    try { FileSystems.delete(sc, dstPath) }
  }

  it should "rename folder" in {
    val srcPath = s"$rootFolder/src_path_$randomString"
    val dstPath = s"$rootFolder/dst_path_$randomString"

    val fileSrcPath = s"$srcPath/a_file.txt"
    val fileDstPath = s"$dstPath/a_file.txt"

    val data = s"data $randomString"

    // create a file
    FileSystems.write(sc, fileSrcPath, data)

    FileSystems.rename(sc, srcPath, dstPath) should equal (true)

    // existence check
    FileSystems.exists(sc, srcPath) should equal (false)
    FileSystems.exists(sc, dstPath) should equal (true)

    // content check
    FileSystems.read(sc, fileDstPath) should equal (data)

    // cleanup
    try { FileSystems.delete(sc, dstPath) }
  }

  it should "overwrite when dst path is already exists" in {
    val srcPath = s"$rootFolder/src_path_$randomString"
    val dstPath = s"$rootFolder/dst_path_$randomString"

    val srcData = s"source data $randomString"
    val dstData = s"destination data $randomString"

    // create a file
    FileSystems.write(sc, srcPath, srcData)
    FileSystems.write(sc, dstPath, dstData)

    FileSystems.rename(sc, srcPath, dstPath) should equal (true)

    // existence check
    FileSystems.exists(sc, srcPath) should equal (false)
    FileSystems.exists(sc, dstPath) should equal (true)

    // content check
    FileSystems.read(sc, dstPath) should equal (srcData)

    // cleanup
    try {
      FileSystems.delete(sc, srcPath)
      FileSystems.delete(sc, dstPath)
    }
  }

  it should "throw an exception when src path does not exists" in {
    val srcPath = s"$rootFolder/src_path_$randomString"
    val dstPath = s"$rootFolder/dst_path_$randomString"

    intercept[IOException] {
      FileSystems.rename(sc, srcPath, dstPath)
    }
  }

  it should "throw an exception if given src path is invalid" in {
    val srcPath = "an_invalid_path://path_to_file"
    val dstPath = s"$rootFolder/dst_path_$randomString"

    intercept[IllegalArgumentException] {
      FileSystems.rename(sc, srcPath, dstPath)
    }
  }

  it should "throw an exception if given dst path is invalid" in {
    val srcPath = s"$rootFolder/src_path_$randomString"
    val dstPath = "an_invalid_path://path_to_file"

    intercept[IllegalArgumentException] {
      FileSystems.rename(sc, srcPath, dstPath)
    }
  }

  //endregion Path Rename Method Spec

  /** Root folder for test cases **/
  private val rootFolder: String = getClass.getResource("/file_system_spec").getPath

  /** Generates a random Alpha Numeric String with a length of 20 **/
  private def randomString: String = Random.alphanumeric.take(20).mkString("")
}
