package com.vngrs.etl.combiners

import com.vngrs.etl.{Combiner, Pipeline}

import scala.reflect.ClassTag

/**
  * Zips `pipeline1` (taken in apply method) with `pipeline2` (taken in constructor),
  * returning key-value pairs with the first element in each [[com.vngrs.etl.Pipeline]],
  * second element in each [[com.vngrs.etl.Pipeline]], etc.
  * Assumes that the two [[com.vngrs.etl.Pipeline]]s have the *same number of
  * partitions* and the *same number of elements in each partition* (e.g. one was made through
  * a map on the other).
  *
  * @param pipeline2 First [[com.vngrs.etl.Pipeline]]
  * @tparam A Type of first [[com.vngrs.etl.Pipeline]]
  * @tparam B Type of second [[com.vngrs.etl.Pipeline]]
  */
final case class Zipper [A: ClassTag, B: ClassTag](pipeline2: Pipeline[B]) extends Combiner[A, (A, B)] {

  /**
    * Runs zip operation.
    *
    * @param pipeline1 Second [[com.vngrs.etl.Pipeline]]
    * @return key-value pairs with the first element in each [[com.vngrs.etl.Pipeline]],
    * second element in each [[com.vngrs.etl.Pipeline]], etc
    */
  override def apply(pipeline1: Pipeline[A]): Pipeline[(A, B)] = Pipeline(pipeline1.rdd.zip(pipeline2.rdd))
}
