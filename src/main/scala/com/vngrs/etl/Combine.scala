package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * [[com.vngrs.etl.Combine]] is an interface for combining two [[com.vngrs.etl.Pipeline]]s into one.
  *
  * @tparam A Type of Input [[com.vngrs.etl.Pipeline]]
  * @tparam B Type of Output [[com.vngrs.etl.Pipeline]]
  */
trait Combine[A, B] {

  /**
    * Runs combine operation.
    *
    * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
    * @return Combined [[com.vngrs.etl.Pipeline]]
    */
  def apply(pipeline2: Pipeline[A]): Pipeline[B]
}

/**
  * Companion object which acts as a Factory.
  */
object Combine {

  /**
    * Produces a [[com.vngrs.etl.Combine]] which applies given function `f` when [[com.vngrs.etl.Combine]]s
    * `apply` method is called
    *
    * @param f Combiner function
    * @param pipeline1 First [[com.vngrs.etl.Pipeline]]
    * @tparam A Type of first input [[com.vngrs.etl.Pipeline]]
    * @tparam B Type of second input [[com.vngrs.etl.Pipeline]]
    * @tparam C Type of output [[com.vngrs.etl.Pipeline]]
    * @return a [[com.vngrs.etl.Pipeline]] [[com.vngrs.etl.Combine]]
    */
  def apply[A: ClassTag, B: ClassTag, C: ClassTag](f: (RDD[A], RDD[B]) => RDD[C],
                                                   pipeline1: Pipeline[A]): Combine[B, C] = new Combine[B, C] {
    /**
      * Runs combine operation by calling function `f` on [[com.vngrs.etl.Pipeline]]s
      * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
      * @return Combined [[com.vngrs.etl.Pipeline]]
      */
    override def apply(pipeline2: Pipeline[B]): Pipeline[C] = Pipeline(f(pipeline1.rdd, pipeline2.rdd))
  }
}
