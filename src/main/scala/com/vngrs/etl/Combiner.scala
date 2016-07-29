package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Combiner[A, B] {
  def apply(pipeline2: Pipeline[A]): Pipeline[B]
}

object Combiner {
  def apply[A: ClassTag, B: ClassTag, C: ClassTag](f: (RDD[A], RDD[B]) => RDD[C], pipeline1: Pipeline[A]): Combiner[B, C] = new Combiner[B, C] {
    override def apply(pipeline2: Pipeline[B]): Pipeline[C] = Pipeline(f(pipeline1.rdd, pipeline2.rdd))
  }

  def union[A: ClassTag](pipeline1: Pipeline[A]): Combiner[A, A] = Union[A](pipeline1)

  def zip[A: ClassTag, B: ClassTag](pipeline1: Pipeline[A]): Combiner[B, (A, B)] = Zip[A, B](pipeline1)
}

final case class Union[A: ClassTag](pipeline1: Pipeline[A]) extends Combiner[A, A] {
  override def apply(pipeline2: Pipeline[A]): Pipeline[A] = Pipeline(pipeline1.rdd.union(pipeline2.rdd))
}

final case class Zip[A: ClassTag, B: ClassTag](pipeline1: Pipeline[A]) extends Combiner[B, (A, B)] {
  override def apply(pipeline2: Pipeline[B]): Pipeline[(A, B)] = Pipeline(pipeline1.rdd.zip(pipeline2.rdd))
}