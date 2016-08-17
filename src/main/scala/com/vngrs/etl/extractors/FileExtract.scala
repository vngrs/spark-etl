package com.vngrs.etl.extractors

import com.vngrs.etl.Extract
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Extracts data as [[org.apache.spark.rdd.RDD]] of [[String]] from file(s).
  */
final case class FileExtract(path: String) extends Extract[String] {

  /**
    * Reads file from given path and returns it as [[org.apache.spark.rdd.RDD]] of [[String]].
    * Minumum partition is set as `defaultMinPartitions` in [[org.apache.spark.SparkContext]].
    *
    * @param sc [[org.apache.spark.SparkContext]]
    * @return Extracted [[org.apache.spark.rdd.RDD]]
    */
  override def apply(sc: SparkContext): RDD[String] = apply(sc, sc.defaultMinPartitions)

  /**
    * Reads file from given path and returns it as [[org.apache.spark.rdd.RDD]] of [[String]].
    *
    * @param sc [[org.apache.spark.SparkContext]]
    * @param minPartitions Minimum partitions for RDD
    * @return Extracted [[org.apache.spark.rdd.RDD]]
    */
  def apply(sc: SparkContext, minPartitions: Int): RDD[String] = sc.textFile(path, minPartitions)
}
