package com.vngrs.etl

import com.vngrs.etl.combiners.{Union, Zip}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Represents ETL Pipeline.
  *
  * @param rdd The [[org.apache.spark.rdd.RDD]] which will be ETLed
  * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
  */
final class Pipeline[A: ClassTag](val rdd: RDD[A]) {

  /**
    * Transforms [[com.vngrs.etl.Pipeline]] with given [[com.vngrs.etl.Transform]].
    *
    * @param transformer [[com.vngrs.etl.Transform]]
    * @tparam B Type of the output [[com.vngrs.etl.Pipeline]]
    * @return Transformed [[com.vngrs.etl.Pipeline]]
    */
  def transform[B: ClassTag](transformer: Transform[A, B]): Pipeline[B] = Pipeline(transformer(rdd))

  /**
    * Loads this [[com.vngrs.etl.Pipeline]] to given [[com.vngrs.etl.Load]](s).
    *
    * @param loaders [[com.vngrs.etl.Load]]
    */
  def load(loaders: Load[A]*): Unit = loaders.foreach(loader => loader(rdd))

  /**
    * Combines this [[com.vngrs.etl.Pipeline]] with another [[com.vngrs.etl.Pipeline]]
    * by given [[com.vngrs.etl.Combine]]
    *
    * @param combiner [[com.vngrs.etl.Combine]]
    * @tparam B Type of another [[com.vngrs.etl.Pipeline]]
    * @return Combined [[com.vngrs.etl.Pipeline]]
    */
  def combine[B: ClassTag, C: ClassTag](combiner: Combine[B, A, C]): Pipeline[C] = combiner(this)

  /**
    * Zips this [[com.vngrs.etl.Pipeline]] with the `other` one.
    *
    * @param other [[com.vngrs.etl.Pipeline]]
    * @tparam B Type of the `other` [[com.vngrs.etl.Pipeline]]
    * @return Zipped [[com.vngrs.etl.Pipeline]]
    */
  def zip[B: ClassTag](other: Pipeline[B]): Pipeline[(A, B)] = Zip[A, B](this).apply(other)

  /**
    * Unions this [[com.vngrs.etl.Pipeline]] with the `other` one.
    *
    * @param other [[com.vngrs.etl.Pipeline]]
    * @return Union of [[com.vngrs.etl.Pipeline]](s)
    */
  def union(other: Pipeline[A]): Pipeline[A] = Union(this).apply(other)
}

/**
  * Companion object which acts as a Factory.
  */
object Pipeline {

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[org.apache.spark.rdd.RDD]].
    *
    * @param rdd [[com.vngrs.etl.Extract]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](rdd: RDD[A]): Pipeline[A] = new Pipeline(rdd)

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[com.vngrs.etl.Extract]].
    *
    * @param extractor [[com.vngrs.etl.Extract]]
    * @param sc [[org.apache.spark.SparkContext]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](extractor: Extract[A])(implicit sc: SparkContext): Pipeline[A] = new Pipeline(extractor(sc))

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[Iterable]].
    *
    * @param it [[com.vngrs.etl.Extract]]
    * @param sc [[org.apache.spark.SparkContext]]
    * @tparam A Type of the [[Iterable]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](it: Iterable[A])(implicit sc: SparkContext): Pipeline[A] = Pipeline(Extract(it))
}
