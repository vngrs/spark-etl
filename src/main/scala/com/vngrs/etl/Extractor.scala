package com.vngrs.etl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Extracts data ([[org.apache.spark.rdd.RDD]]) from different sources.
  *
  * @tparam A Type of extracted the data ([[org.apache.spark.rdd.RDD]]).
  */
trait Extractor[A] {

  /**
    * Runs extraction operation.
    *
    * @param sc [[org.apache.spark.SparkContext]]
    * @return Extracted [[org.apache.spark.rdd.RDD]]
    */
  def apply(sc: SparkContext): RDD[A]
}

/**
  * Companion object which acts as a Factory.
  */
// Since this is a factory object, overloading warning has been suppressed
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object Extractor {

  /**
    * Creates an [[com.vngrs.etl.Extractor]] from given [[org.apache.spark.rdd.RDD]].
    *
    * @param rdd [[org.apache.spark.rdd.RDD]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Extractor]]
    */
  def apply[A](rdd: RDD[A]): Extractor[A] = new Extractor[A] {

    /**
      * Returns given [[org.apache.spark.rdd.RDD]].
      *
      * @param sc [[org.apache.spark.SparkContext]]
      * @return Given [[org.apache.spark.rdd.RDD]]
      */
    override def apply(sc: SparkContext): RDD[A] = rdd
  }

  /**
    * Creates an [[com.vngrs.etl.Extractor]] from given [[Iterable]].
    *
    * @param it [[Iterable]]
    * @tparam A Type of the [[Iterable]]
    * @return [[com.vngrs.etl.Extractor]]
    */
  def apply[A: ClassTag](it: Iterable[A]): Extractor[A] = new Extractor[A] {

    /**
      * Parallelize given [[Iterable]].
      *
      * @param sc [[org.apache.spark.SparkContext]]
      * @return Extracted [[org.apache.spark.rdd.RDD]]
      */
    override def apply(sc: SparkContext): RDD[A] = sc.parallelize[A](it.toSeq)
  }
}
