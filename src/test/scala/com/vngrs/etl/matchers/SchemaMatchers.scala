package com.vngrs.etl.matchers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.matchers.{MatchResult, Matcher}

/**
  * Helper Schema matchers for [[org.apache.spark.sql.Row]]
  */
object SchemaMatchers {

  /**
    * Asserts whether `left` (`RDD[Row]`) has same field names with `right` (`Set[String]`).
    *
    * @param right Field names
    * @return [[org.scalatest.matchers.MatchResult]]
    */
  def hasSameFieldNamesWith(right: Set[String]): Matcher[RDD[Row]] = new Matcher[RDD[Row]] {
    override def apply(left: RDD[Row]): MatchResult = {
      MatchResult(
        left.first().schema.fields.map(_.name).toSet.equals(right),
        "Given field names was not same with the first row schema",
        "Given field names was same with the first row schema"
      )
    }
  }

  /**
    * Asserts whether `left` (`RDD[Row]`) has same schema with `right` (`StructType`).
    *
    * @param right Schema
    * @return [[org.scalatest.matchers.MatchResult]]
    */
  def hasSameSchemaWith(right: StructType): Matcher[RDD[Row]] = new Matcher[RDD[Row]] {
    override def apply(left: RDD[Row]): MatchResult = {
      MatchResult(
        left.first().schema.equals(right),
        "Given schema is not same with the first row of the RDD",
        "Given schema is same with the first row of the RDD"
      )
    }
  }

  /**
    * Asserts whether `left` (`RDD[Row]`) has same fields with `right` (`Set[StructField]`).
    *
    * @param right Fields
    * @return [[org.scalatest.matchers.MatchResult]]
    */
  def hasSameFieldsWith(right: Set[StructField]): Matcher[RDD[Row]] = new Matcher[RDD[Row]] {
    override def apply(left: RDD[Row]): MatchResult = {
      MatchResult(
        left.first().schema.fields.toSet.equals(right),
        "Given set of fields is not same with the first row of the RDD",
        "Given set of fields is same with the first row of the RDD"
      )
    }
  }
}
