package com.vngrs.etl

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll

import scala.util.Random

/**
  * Created by furkanvarol on 29/07/16.
  */
abstract class BaseSparkSpec extends BaseSpec with BeforeAndAfterAll {
  private val conf = new SparkConf().setMaster("local[*]").setAppName(generateAppName())
  private val sc = new SparkContext(conf)

  override def afterAll(): Unit = {
    sc.stop()
  }

  private def generateAppName(): String = Random.nextString(16)

  implicit class ExtractorUnderTest[A](e: Extractor[A]) {

    def testRun: Seq[A] = e(sc).collect()

  }

}
