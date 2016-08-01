package com.vngrs.etl

import org.apache.spark.rdd.RDD

/**
  * Loads data ([[org.apache.spark.rdd.RDD]]) to different sources.
  *
  * @tparam A Type of loaded the data ([[org.apache.spark.rdd.RDD]]).
  */
trait Load[A] {

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
object Load {

  /**
    * Creates a [[com.vngrs.etl.Load]] by calling given function `f` on given data.
    *
    * @param f Function to call
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Load]]
    */
  def apply[A](f: RDD[A] => Unit): Load[A] = new Load[A]() {

    /**
      * Loads data by calling function `f`
      *
      * @param data [[org.apache.spark.rdd.RDD]]
      */
    override def apply(data: RDD[A]): Unit = f(data)
  }
}
