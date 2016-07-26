package com.vngrs.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait Loader[A] {

  def apply(input: RDD[A]): Unit

}

object Loader {
  def apply[A](f: RDD[A] => Unit): Loader[A] = new Loader[A]() {
    override def apply(data: RDD[A]): Unit = f(data)
  }

  def file(path: String): Loader[String] = new Loader[String] {
    override def apply(input: RDD[String]): Unit = input.saveAsTextFile(path)
  }

  def redshift(): Loader[Row] = new Loader[Row] {
    override def apply(input: RDD[Row]): Unit = { /*  */ }
  }

}