package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * [[com.vngrs.etl.Combine]] is an interface for combining two [[com.vngrs.etl.Pipeline]]s into one.
  *
  * @tparam A Type of First Input [[com.vngrs.etl.Pipeline]]
  * @tparam B Type of Second Input [[com.vngrs.etl.Pipeline]]
  * @tparam C Type of Output [[com.vngrs.etl.Pipeline]]
  */
abstract class Combine[A: ClassTag, B: ClassTag, C: ClassTag](pipeline1: Pipeline[A]) {

  /**
    * Runs combine operation.
    *
    * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
    * @return Combined [[com.vngrs.etl.Pipeline]]
    */
  def apply(pipeline2: Pipeline[B]): Pipeline[C]
}

/**
  * Companion object which acts as a Factory.
  */
object Combine {

  /**
    * Produces a [[com.vngrs.etl.Combine]] which applies given function `f`
    * when [[com.vngrs.etl.Combine]]s `apply` method is called
    *
    * @param f Combiner function
    * @param pipeline1 First [[com.vngrs.etl.Pipeline]]
    * @tparam A Type of first input [[com.vngrs.etl.Pipeline]]
    * @tparam B Type of second input [[com.vngrs.etl.Pipeline]]
    * @tparam C Type of output [[com.vngrs.etl.Pipeline]]
    * @return a [[com.vngrs.etl.Pipeline]] [[com.vngrs.etl.Combine]]
    */
  def apply[A: ClassTag, B: ClassTag, C: ClassTag](
      f: (RDD[A], RDD[B]) => RDD[C],
      pipeline1: Pipeline[A]
  ): Combine[A, B, C] = new Combine[A, B, C](pipeline1) {

    /**
      * Runs combine operation by calling function `f` on [[com.vngrs.etl.Pipeline]]s
      * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
      * @return Combined [[com.vngrs.etl.Pipeline]]
      */
    override def apply(pipeline2: Pipeline[B]): Pipeline[C] = Pipeline(f(pipeline1.rdd, pipeline2.rdd))
  }
}
