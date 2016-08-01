package com.vngrs.etl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Extracts data ([[org.apache.spark.rdd.RDD]]) from different sources.
  *
  * @tparam A Type of extracted the data ([[org.apache.spark.rdd.RDD]]).
  */
trait Extract[A] {

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
object Extract {

  /**
    * Creates an [[com.vngrs.etl.Extract]] from given [[org.apache.spark.rdd.RDD]].
    *
    * @param rdd [[org.apache.spark.rdd.RDD]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Extract]]
    */
  def apply[A](rdd: RDD[A]): Extract[A] = new Extract[A] {

    /**
      * Returns given [[org.apache.spark.rdd.RDD]].
      *
      * @param sc [[org.apache.spark.SparkContext]]
      * @return Given [[org.apache.spark.rdd.RDD]]
      */
    override def apply(sc: SparkContext): RDD[A] = rdd
  }

  /**
    * Creates an [[com.vngrs.etl.Extract]] from given [[Iterable]].
    *
    * @param it [[Iterable]]
    * @tparam A Type of the [[Iterable]]
    * @return [[com.vngrs.etl.Extract]]
    */
  def apply[A: ClassTag](it: Iterable[A]): Extract[A] = new Extract[A] {

    /**
      * Parallelize given [[Iterable]].
      *
      * @param sc [[org.apache.spark.SparkContext]]
      * @return Extracted [[org.apache.spark.rdd.RDD]]
      */
    override def apply(sc: SparkContext): RDD[A] = sc.parallelize[A](it.toSeq)
  }

  /**
    * Creates an Extractor by calling given function `f`
    *
    * @param f Function to supply [[org.apache.spark.rdd.RDD]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Extract]]
    */
  def apply[A: ClassTag](f: SparkContext => RDD[A]): Extract[A] = new Extract[A] {

    /**
      * Runs given function `f`
      *
      * @param sc [[org.apache.spark.SparkContext]]
      * @return Extracted [[org.apache.spark.rdd.RDD]]
      */
    override def apply(sc: SparkContext): RDD[A] = f(sc)
  }
}
