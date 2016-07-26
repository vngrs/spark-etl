package com.vngrs.etl

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


trait Transformer[A, B] {

  def apply(input: RDD[A]): RDD[B]

}

object Transformer {

  def apply[A: ClassTag, B: ClassTag](f: A => B): Transformer[A, B] = new Transformer[A, B] {
    override def apply(input: RDD[A]): RDD[B] = input.map(f)
  }

  def filter[A: ClassTag](f: A => Boolean): Transformer[A, A] = new Transformer[A, A] {
    override def apply(input: RDD[A]): RDD[A] = input.filter(f)
  }

}