package com.vngrs.etl.combiners

import com.vngrs.etl.{Pipeline, SparkSpec}
import org.apache.spark.SparkException

/**
  * Zip Combiner Specifications
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ZipSpec extends SparkSpec {

  "A Zip Combiner" should "accept two Pipeline with Empty RDDs" in {
    val pipeline1 = new Pipeline(emptyRDD[Int])
    val pipeline2 = new Pipeline(emptyRDD[Int])

    val zipCombiner = Zip[Int, Int](pipeline1)

    val combinedPipeline = zipCombiner(pipeline2)

    combinedPipeline.testCollect.length should equal (0)
  }

  it should "union any two Pipeline with non empty RDDs" in {
    val pipeline1 = new Pipeline(parallelize(List(1, 2, 3)))
    val pipeline2 = new Pipeline(parallelize(List(3, 4, 5)))

    val zipCombiner = Zip[Int, Int](pipeline1)

    val combinedPipeline = zipCombiner(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List((1, 3), (2, 4), (3, 5))
  }

  it should "accept same Pipeline with non empty RDD to populate its elements" in {
    val pipeline = new Pipeline(parallelize(List(1, 2, 3)))

    val zipCombiner = Zip[Int, Int](pipeline)

    val combinedPipeline = zipCombiner(pipeline)

    combinedPipeline.testCollect should contain theSameElementsAs List((1, 1), (2, 2), (3, 3))
  }

  it should "give an exception when Pipelines' lengths are not same" in {
    intercept[SparkException] {
      val pipeline1 = new Pipeline(parallelize(List(1, 2, 3)))
      val pipeline2 = new Pipeline(parallelize(List(1, 2, 3, 4)))

      val zipCombiner = Zip[Int, Int](pipeline1)

      val combinedPipeline = zipCombiner(pipeline2)

      combinedPipeline.testCollect
    }
  }
}
