package com.vngrs.etl.utils

import com.vngrs.etl.exceptions.NoSchemaException
import com.vngrs.etl.utils.rdd.SchemaResult.{EmptyRdd, Found, NotFound}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

/**
  * Enriches [[org.apache.spark.rdd.RDD]] with more functionality
  */
object rdd {

  /**
    * Enriches [[org.apache.spark.rdd.RDD]] to have more functionality
    *
    * @param rdd [[org.apache.spark.rdd.RDD]]
    * @tparam T Type of the RDD
    */
  implicit final class RichRDD[T](val rdd: RDD[T]) extends AnyVal {

    /**
      * Checks whether RDD is non empty or not
      * @return
      */
    def nonEmpty(): Boolean = !rdd.isEmpty()

    /**
      * Creates a new [[org.apache.spark.sql.SQLContext]] with [[org.apache.spark.SparkContext]] inside the `rdd`.
      *
      * @return newly created [[org.apache.spark.sql.SQLContext]]
      */
    def sqlContext: SQLContext = new SQLContext(rdd.sparkContext)
  }

  /**
    * Enriches [[org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    * to have more functionality
    *
    * @param rdd [[org.apache.spark.rdd.RDD]]
    */
  implicit final class RichRowRDD(val rdd: RDD[Row]) extends AnyVal {

    /**
      * Fetches Schema
      *
      * @return [[com.vngrs.etl.utils.rdd.SchemaResult]]
      */
    def schema(): SchemaResult = {
      Try(rdd.first()) match {
        case Success(r) => Option(r.schema).map(s => Found(s)).getOrElse[SchemaResult](NotFound)
        case Failure(e) => EmptyRdd
      }
    }
  }

  /**
    * Schema Result for fetching schema in [org.apache.spark.rdd.RDD]] of [[org.apache.spark.sql.Row]]s
    */
  sealed trait SchemaResult {

    /**
      * Folds schema results with given event functions
      *
      * @param onFound will be called when a schema is found ([[com.vngrs.etl.utils.rdd.SchemaResult.Found]])
      * @param onNotFound will be called when no schema is found ([[com.vngrs.etl.utils.rdd.SchemaResult.NotFound]])
      * @param onEmptyRdd will be called when `rdd` is empty ([[com.vngrs.etl.utils.rdd.SchemaResult.EmptyRdd]])
      * @tparam A Type of the return
      * @return result of the event functions
      */
    def fold[A](onFound: StructType => A, onNotFound: => A, onEmptyRdd: => A): A = {
      this match {
        case Found(s) => onFound(s)
        case NotFound => onNotFound
        case EmptyRdd => onEmptyRdd
      }
    }

    /**
      * Gets Schema of the first [[org.apache.spark.sql.Row]] as [[scala.Option]]
      *
      * @return Schema of the rows if rdd is non empty and rows have a schema, otherwise `None`
      */
    def toOption: Option[StructType] = fold(Some(_), None, None)

    /**
      * Gets Schema of the first [[org.apache.spark.sql.Row]] as [[scala.Option]]
      * or throws [[com.vngrs.etl.exceptions.NoSchemaException]].
      *
      * @param msg Exception Message
      * @return Schema of the rows if rdd is non empty and rows have a schema
      * @throws com.vngrs.etl.exceptions.NoSchemaException if no schema is found
      */
    @throws(classOf[NoSchemaException])
    def orThrow(msg: String): StructType = toOption.getOrElse(throw new NoSchemaException(msg))
  }

  /**
    * Companion object for SchemaResult
    */
  object SchemaResult {

    /**
      * Schema Found Result which wraps schema
      *
      * @param schema Schema of the data
      */
    final case class Found(schema: StructType) extends SchemaResult

    /**
      * Schema Not Found Result
      */
    case object NotFound extends SchemaResult

    /**
      * Empty RDD Result
      */
    case object EmptyRdd extends SchemaResult
  }
}
