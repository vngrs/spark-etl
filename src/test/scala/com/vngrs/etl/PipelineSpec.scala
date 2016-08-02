package com.vngrs.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

/**
  * Pipeline Specifications
  */
// Following wart does not work with scalatest's intercept functionality
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class PipelineSpec extends SparkSpec {
  "A Pipeline" should "accept an Empty RDD" in {
    val rdd = emptyRDD[Int]

    val pipeline = new Pipeline(rdd)

    pipeline.rdd should equal (rdd)
  }

  it should "keep same any data taken by its constructor" in {
    val data = List(1, 2, 3)
    val rdd = parallelize(data)

    val pipeline = new Pipeline(rdd)

    pipeline.testCollect should contain theSameElementsAs data
  }

  it should "transform data" in {
    val rdd = parallelize(List(1, 2, 3))

    val pipeline = new Pipeline(rdd)

    val transformedPipeline = pipeline.transform(Transform(rdd => rdd.map(i => Math.pow(i, 2).toInt)))

    transformedPipeline.testCollect should contain theSameElementsAs List(1, 4, 9)
  }

  it should "load data to one destination" in {
    val rdd = parallelize(List("a", "b", "c"))
    val sbStream = new StringBuilder()

    val pipeline = new Pipeline(rdd)

    pipeline.load(Load(_.collect().foreach(sbStream.append)))

    sbStream.toString() should equal ("abc")
  }

  it should "load data to multiple destinations" in {
    val rdd = parallelize(List("a", "b", "c"))
    val sbStream1 = new StringBuilder()
    val sbStream2 = new StringBuilder()

    val pipeline = new Pipeline(rdd)

    pipeline.load(
      Load(_.collect().foreach(sbStream1.append)),
      Load(_.collect().foreach(sbStream2.append))
    )

    sbStream1.toString() should equal ("abc")
    sbStream2.toString() should equal ("abc")
  }

  it should "accept a functionless combiner and result should be itself" in {
    val rdd = parallelize(List(1, 2, 3))

    val pipeline = new Pipeline(rdd)

    val combinedPipeline = pipeline.combine(new Combine[Int, Int, Int](pipeline) {
      override def apply(pipeline2: Pipeline[Int]): Pipeline[Int] = pipeline
    })

    combinedPipeline should equal (pipeline)
  }

  it should "be able to union with it self" in {
    val rdd = parallelize(List(1, 2, 3))

    val pipeline = new Pipeline(rdd)

    val combinedPipeline = pipeline.union(pipeline)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 1, 2, 3)
  }

  it should "be able to union with another Pipeline" in {
    val rdd1 = parallelize(List(1, 2, 3))
    val rdd2 = parallelize(List(3, 4, 5))

    val pipeline1 = new Pipeline(rdd1)
    val pipeline2 = new Pipeline(rdd2)

    val combinedPipeline = pipeline1.union(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 3, 4, 5)
  }

  it should "be able to union with another Pipeline when lengths are variable" in {
    val rdd1 = parallelize(List(1, 2, 3))
    val rdd2 = parallelize(List(1, 2, 3, 4))

    val pipeline1 = new Pipeline(rdd1)
    val pipeline2 = new Pipeline(rdd2)

    val combinedPipeline = pipeline1.union(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 1, 2, 3, 4)
  }

  it should "be able to zip with it self" in {
    val rdd = parallelize(List(1, 2, 3))

    val pipeline = new Pipeline(rdd)

    val combinedPipeline = pipeline.zip(pipeline)

    combinedPipeline.testCollect should contain theSameElementsAs List((1, 1), (2, 2), (3, 3))
  }

  it should "be able to zip with another Pipeline" in {
    val rdd1 = parallelize(List(1, 2, 3))
    val rdd2 = parallelize(List(3, 4, 5))

    val pipeline1 = new Pipeline(rdd1)
    val pipeline2 = new Pipeline(rdd2)

    val combinedPipeline = pipeline1.zip(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List((1, 3), (2, 4), (3, 5))
  }

  it should "give an exception when Pipelines' lengths are not same" in {
    intercept[SparkException] {
      val pipeline1 = new Pipeline(parallelize(List(1, 2, 3)))
      val pipeline2 = new Pipeline(parallelize(List(1, 2, 3, 4)))

      val combinedPipeline = pipeline1.zip(pipeline2)

      combinedPipeline.testCollect
    }
  }

  "Pipeline's Companion Object" should "create a Pipeline with an empty RDD" in {
    val rdd = emptyRDD[Int]

    val pipeline = Pipeline(rdd)

    pipeline.rdd should equal (rdd)
  }

  it should "create a Pipeline with any non empty RDD" in {
    val rdd = parallelize(List(1, 2, 3))

    val pipeline = Pipeline(rdd)

    pipeline.rdd should equal (rdd)
  }

  it should "create a Pipeline with an empty Iterable" in {
    val iterable = List[Int]()

    val pipeline = Pipeline(iterable)

    pipeline.testCollect.length should equal (0)
  }

  it should "create a Pipeline with any non empty Iterable" in {
    val iterable = List(1, 2, 3)

    val pipeline = Pipeline(iterable)

    pipeline.testCollect should contain theSameElementsAs iterable
  }

  it should "create a Pipeline from an Extractor with an empty RDD" in {
    val rdd = emptyRDD[Int]
    val extractor = new Extract[Int] {
      override def apply(sc: SparkContext): RDD[Int] = rdd
    }

    val pipeline = Pipeline(extractor)

    pipeline.testCollect.length should equal (0)
  }

  it should "create a Pipeline from an Extractor with any non empty RDD" in {
    val data = List(1, 2, 3)
    val rdd = parallelize(data)
    val extractor = new Extract[Int] {
      override def apply(sc: SparkContext): RDD[Int] = rdd
    }

    val pipeline = Pipeline(extractor)

    pipeline.testCollect should contain theSameElementsAs data
  }
}
