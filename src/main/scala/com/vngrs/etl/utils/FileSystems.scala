package com.vngrs.etl.utils

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import scala.io.Source

/**
  * File System Utility Methods via `Hadoop File System` interface.
  */
object FileSystems {

  /**
    * Checks existence of given `path`.
    *
    * @param sc Spark Context
    * @param path Path
    * @return `true` if path exists, `false` otherwise
    * @throws IllegalArgumentException if given path has invalid character
    */
  @throws(classOf[IllegalArgumentException])
  def exists(sc: SparkContext, path: String): Boolean = {
    val pathObj = new Path(path)
    val fs = pathObj.getFileSystem(sc.hadoopConfiguration)

    fs.exists(pathObj)
  }

  /**
    * Reads given `path` and returns it as a whole string
    *
    * @param sc Spark Context
    * @param path Path
    * @return Read String (UTF-8)
    * @throws IllegalArgumentException if given path has invalid character
    * @throws IOException if given path does not exists
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IOException])
  def read(sc: SparkContext, path: String): String = {
    val pathObj = new Path(path)
    val fs = pathObj.getFileSystem(sc.hadoopConfiguration)

    val is = fs.open(pathObj)
    val data = Source.fromInputStream(is, "UTF-8").getLines().mkString("\n")

    is.close()

    data
  }

  /**
    * Reads given `path` and returns it as a whole string if exits.
    * If not, writes given `defValue` to the given `path` and return the `defValue`.
    *
    * @param sc Spark Context
    * @param path Path
    * @param defValue Default Value
    * @return Read String (UTF-8)
    * @throws IllegalArgumentException if given path has invalid character
    */
  @throws(classOf[IllegalArgumentException])
  def readWithDefault(sc: SparkContext, path: String, defValue: => String): String = {
    val pathObj = new Path(path)
    val fs = pathObj.getFileSystem(sc.hadoopConfiguration)

    if (!fs.exists(pathObj)) {
      val os = fs.create(pathObj)
      os.write(defValue.getBytes("UTF-8"))
      os.close()
    }

    val is = fs.open(pathObj)
    val data = Source.fromInputStream(is, "UTF-8").getLines().mkString("\n")

    is.close()

    data
  }

  /**
    * Writes given `data` to given `path`. Overwrites file if exists.
    *
    * @param sc Spark Context
    * @param path Path
    * @param data Data (UTF-8)
    * @throws IllegalArgumentException if given path has invalid character
    */
  @throws(classOf[IllegalArgumentException])
  def write(sc: SparkContext, path: String, data: String): Unit = write(sc, path, data, overwrite = true)

  /**
    * Writes given `data` to given `path`.
    *
    * @param sc Spark Context
    * @param path Path
    * @param data Data (UTF-8)
    * @param overwrite if a file with this name already exists, then if true,
    *   the file will be overwritten, and if false an exception will be thrown.
    * @throws IllegalArgumentException if given path has invalid character
    * @throws java.io.IOException if given path does not exists
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IOException])
  def write(sc: SparkContext, path: String, data: String, overwrite: Boolean): Unit = {
    val pathObj = new Path(path)
    val fs = pathObj.getFileSystem(sc.hadoopConfiguration)

    val os = fs.create(pathObj, overwrite)

    os.write(data.getBytes("UTF-8"))

    os.close()
  }

  /**
    * Deletes given `path` recursively.
    *
    * @param sc Spark Context
    * @param path Path
    * @return `true` if successful, `false` otherwise.
    * @throws IllegalArgumentException if given path has invalid character
    */
  @throws(classOf[IllegalArgumentException])
  def delete(sc: SparkContext, path: String): Boolean = delete(sc, path, recursive = true)

  /**
    * Deletes given `path`.
    *
    * @param sc Spark Context
    * @param path Path
    * @param recursive Recursive Flag
    * @return `true` if successful, `false` otherwise.
    * @throws IllegalArgumentException if given path has invalid character
    */
  @throws(classOf[IllegalArgumentException])
  def delete(sc: SparkContext, path: String, recursive: Boolean): Boolean = {
    val pathObj = new Path(path)
    val fs = pathObj.getFileSystem(sc.hadoopConfiguration)

    fs.delete(pathObj, recursive)
  }

  /**
    * Renames given `src` path to `dst` path
    *
    * @param sc Spark Context
    * @param src Source
    * @param dst Destination
    * @return `true` if successful, `false` otherwise.
    * @throws IllegalArgumentException if given path has invalid character
    * @throws java.io.IOException if given path does not exists
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IOException])
  def rename(sc: SparkContext, src: String, dst: String): Boolean = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    val fs = srcPath.getFileSystem(sc.hadoopConfiguration)

    fs.rename(srcPath, dstPath)
  }
}
