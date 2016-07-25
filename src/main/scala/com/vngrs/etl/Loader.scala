package com.vngrs.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait Loader[A] {

  protected[etl] def apply(input: RDD[A]): Loader[A]

}

object Loader {
  def apply[A](f: RDD[A] => Unit): Loader[A] = new Loader[A]() {
    override def apply(data: RDD[A]): Loader[A] = {
      f(data)
      this
    }
  }

  def file(path: String): Loader[String] = new Loader[String] {
    override protected[etl] def apply(input: RDD[String]): Loader[String] = {
      input.saveAsTextFile(path)
      this
    }
  }

  def redshift(): Loader[Row] = new Loader[Row] {
    protected[etl] def apply(input: RDD[Row]): Loader[Row] = this
  }

}