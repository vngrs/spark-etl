package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by furkanvarol on 26/07/16.
  */
trait Aggregator[A, B, C] {
  def apply(rdd1: Pipeline[A], rdd2: Pipeline[B]): Pipeline[C]
}

object Aggregator {
  def apply[A: ClassTag, B: ClassTag, C: ClassTag](f: (RDD[A], RDD[B]) => RDD[C]): Aggregator[A, B, C] = new Aggregator[A, B, C] {
    override def apply(pipeline1: Pipeline[A], pipeline2: Pipeline[B]): Pipeline[C] = Pipeline(f(pipeline1.rdd, pipeline2.rdd))
  }

  def union[A: ClassTag](pipeline1: Pipeline[A], pipeline2: Pipeline[A]): Aggregator[A, A, A] = new Aggregator[A, A, A] {
    override def apply(pipeline1: Pipeline[A], pipeline2: Pipeline[A]): Pipeline[A] = Pipeline(pipeline1.rdd.union(pipeline2.rdd))
  }

  def zip[A: ClassTag, B: ClassTag](pipeline1: Pipeline[A], pipeline2: Pipeline[B]): Aggregator[A, B, (A, B)] = new Aggregator[A, B, (A, B)] {
    override def apply(pipeline1: Pipeline[A], pipeline2: Pipeline[B]): Pipeline[(A, B)] = Pipeline(pipeline1.rdd.zip(pipeline2.rdd))
  }
}
