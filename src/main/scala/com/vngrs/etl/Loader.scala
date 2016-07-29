package com.vngrs.etl

import org.apache.spark.rdd.RDD

/**
  * Loads data ([[org.apache.spark.rdd.RDD]]) to different sources.
  *
  * @tparam A Type of loaded the data ([[org.apache.spark.rdd.RDD]]).
  */
trait Loader[A] {

  /**
    * Runs loading operation.
    *
    * @param input [[org.apache.spark.rdd.RDD]]
    */
  def apply(input: RDD[A]): Unit
}

/**
  * Companion object which acts as a Factory.
  */
// Since this is a factory object, overloading warning has been suppressed
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object Loader {

  /**
    * Creates a [[com.vngrs.etl.Loader]] by calling given function `f` on given data.
    *
    * @param f Function to call
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Loader]]
    */
  def apply[A](f: RDD[A] => Unit): Loader[A] = new Loader[A]() {

    /**
      * Loads data by calling function `f`
      *
      * @param data [[org.apache.spark.rdd.RDD]]
      */
    override def apply(data: RDD[A]): Unit = f(data)
  }
}
