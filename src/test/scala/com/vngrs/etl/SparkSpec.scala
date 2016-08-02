package com.vngrs.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Base Specification class which handles SparkContext creation and demolition
  */
abstract class SparkSpec extends BaseSpec with BeforeAndAfterAll {
  private val conf = new SparkConf().setMaster("local[*]").setAppName(Random.nextString(16))
  // TODO make SparkContext private but also provide a way to supply it to Pipeline implicitly
  protected implicit val sc: SparkContext = new SparkContext(conf)

  override def afterAll(): Unit = {
    sc.stop()
  }

  protected implicit class ExtractUnderTest[A: ClassTag](e: Extract[A]) {

    def testRun: RDD[A] = e(sc)

    def testCollect: Seq[A] = testRun.collect()
  }

  protected implicit class PipelineUnderTest[A: ClassTag](p: Pipeline[A]) {

    def testCollect: Seq[A] = p.rdd.collect()
  }

  protected def parallelize[A: ClassTag](seq: Seq[A]): RDD[A] = sc.parallelize[A](seq)

  protected def emptyRDD[A: ClassTag]: RDD[A] = sc.emptyRDD
}
