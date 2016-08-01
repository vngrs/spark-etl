package com.vngrs.etl.combiners

import com.vngrs.etl.{Combine, Pipeline}

import scala.reflect.ClassTag

/**
  * Unions `pipeline1` (taken in constructor) with `pipeline2` (taken in apply method),
  * returning new [[com.vngrs.etl.Pipeline]] with all of the element.
  * Both of [[com.vngrs.etl.Pipeline]]s must have same type.
  *
  * @param pipeline1 First [[com.vngrs.etl.Pipeline]]
  * @tparam A Type of [[com.vngrs.etl.Pipeline]]s
  */
final case class Union[A: ClassTag](pipeline1: Pipeline[A]) extends Combine[A, A] {

  /**
    * Runs union operation.
    *
    * @param pipeline2 Second [[com.vngrs.etl.Pipeline]]
    * @return the union of [[com.vngrs.etl.Pipeline]]s. Any identical elements will appear multiple times.
    */
  override def apply(pipeline2: Pipeline[A]): Pipeline[A] = Pipeline(pipeline1.rdd.union(pipeline2.rdd))
}
