package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * [[com.vngrs.etl.Combiner]] is an interface for combining two [[com.vngrs.etl.Pipeline]]s into one.
  *
  * @tparam A Type of Input [[com.vngrs.etl.Pipeline]]
  * @tparam B Type of Output [[com.vngrs.etl.Pipeline]]
  */
trait Combiner[A, B] {

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
// Since this is a factory object, overloading warning has been suppressed
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object Combiner {

  /**
    * Produces a [[com.vngrs.etl.Combiner]] which applies given function `f` when [[com.vngrs.etl.Combiner]]s
    * `apply` method is called
    *
    * @param f Combiner function
    * @param pipeline1 First [[com.vngrs.etl.Pipeline]]
    * @tparam A Type of first input [[com.vngrs.etl.Pipeline]]
    * @tparam B Type of second input [[com.vngrs.etl.Pipeline]]
    * @tparam C Type of output [[com.vngrs.etl.Pipeline]]
    * @return a [[com.vngrs.etl.Pipeline]] [[com.vngrs.etl.Combiner]]
    */
  def apply[A: ClassTag, B: ClassTag, C: ClassTag](f: (RDD[A], RDD[B]) => RDD[C],
                                                   pipeline1: Pipeline[A]): Combiner[B, C] = new Combiner[B, C] {
    /**
      * Runs combine operation by calling function `f` on [[com.vngrs.etl.Pipeline]]s
      * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
      * @return Combined [[com.vngrs.etl.Pipeline]]
      */
    override def apply(pipeline2: Pipeline[B]): Pipeline[C] = Pipeline(f(pipeline1.rdd, pipeline2.rdd))
  }
}
