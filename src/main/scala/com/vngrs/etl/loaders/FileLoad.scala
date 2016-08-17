package com.vngrs.etl.loaders

import com.vngrs.etl.Load
import org.apache.spark.rdd.RDD

/**
  * Loads to given path as file
  */
final case class FileLoad(path: String) extends Load[String] {

  /**
    * Loads given data to given path as file
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    */
  override def apply(input: RDD[String]): Unit = input.saveAsTextFile(path)
}
