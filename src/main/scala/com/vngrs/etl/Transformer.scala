package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Transforms data ([[org.apache.spark.rdd.RDD]]) to another form.
  *
  * @tparam A Type of the data ([[org.apache.spark.rdd.RDD]]).
  */
trait Transformer[A, B] {

  /**
    * Runs transformation operation,
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    * @return Transformed [[org.apache.spark.rdd.RDD]]
    */
  def apply(input: RDD[A]): RDD[B]
}

/**
  * Companion object which acts as a Factory.
  */
// Since this is a factory object, overloading warning has been suppressed
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object Transformer {

  /**
    * Creates a mapper [[com.vngrs.etl.Transformer]] by calling function `f` on given `data`.
    *
    * @param f Mapper function
    * @tparam A Type of the input data [[org.apache.spark.rdd.RDD]]
    * @tparam B Type of the ouput data [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Transformer]]
    */
  def apply[A: ClassTag, B: ClassTag](f: A => B): Transformer[A, B] = new Transformer[A, B] {
    /**
      * Maps whole `input` with function `f`.
      *
      * @param input [[org.apache.spark.rdd.RDD]]
      * @return Transformed [[org.apache.spark.rdd.RDD]]
      */
    override def apply(input: RDD[A]): RDD[B] = input.map(f)
  }

  /**
    * Creates a filterer [com.vngrs.etl.Transformer]] by calling function `f` on given `data`
    *
    * @param f Filterer function
    * @tparam A Type of the data [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Transformer]]
    */
  def filter[A: ClassTag](f: A => Boolean): Transformer[A, A] = new Transformer[A, A] {

    /**
      * Filters `input` with given function `f`.
      *
      * @param input [[org.apache.spark.rdd.RDD]]
      * @return Filtered [[org.apache.spark.rdd.RDD]]
      */
    override def apply(input: RDD[A]): RDD[A] = input.filter(f)
  }

}
