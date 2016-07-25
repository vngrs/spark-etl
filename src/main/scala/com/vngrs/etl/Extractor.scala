package com.vngrs.etl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

trait Extractor[A] {
  protected[etl] def apply(sc: SparkContext): RDD[A]
}

object Extractor {

  def apply[A](rdd: RDD[A]): Extractor[A] = new Extractor[A] {
    override protected[etl] def apply(sc: SparkContext): RDD[A] = rdd
  }

  def apply[A: ClassTag](it: Iterable[A]): Extractor[A] = new Extractor[A] {
    override protected[etl] def apply(sc: SparkContext): RDD[A] = sc.parallelize(it.toSeq)
  }

  def mysql(): Extractor[Row] = new Extractor[Row] {
    override protected[etl] def apply(sc: SparkContext): RDD[Row] = sc.emptyRDD[Row]
  }

}