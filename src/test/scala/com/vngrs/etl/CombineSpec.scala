package com.vngrs.etl

/**
  * Combiner Specifications
  */
class CombineSpec extends BaseSparkSpec {

  "A functionless Combiner" should "return given Pipeline with an Empty RDD as it is" in {
    val rdd = emptyRDD[String]
    val pipeline = new Pipeline(rdd)

    val combiner = new Combine[String, String] {
      override def apply(pipeline2: Pipeline[String]): Pipeline[String] = pipeline2
    }

    combiner(pipeline) should equal (pipeline)
  }

  it should "return given Pipeline with any non empty RDD without changing data in it" in {
    val data = List("a", "b", "c")
    val rdd = parallelize(data)
    val pipeline = new Pipeline(rdd)

    val combiner = new Combine[String, String] {
      override def apply(pipeline2: Pipeline[String]): Pipeline[String] = pipeline2
    }

    combiner(pipeline).testCollect should contain theSameElementsAs data
  }

  "A counter Combiner" should "accept a Pipeline with an empty RDD" in {
    val pipeline1 = new Pipeline(emptyRDD[Int])
    val pipeline2 = new Pipeline(emptyRDD[Int])

    val combiner = Combine[Int, Int, Long]((rdd1, rdd2) => parallelize(List(rdd1.count(), rdd2.count())), pipeline1)

    val combinedPipeline = combiner(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(0, 0)
  }

  it should "count any two Pipeline with non empty RDD" in {
    val pipeline1 = new Pipeline(parallelize(List(1, 2)))
    val pipeline2 = new Pipeline(parallelize(List(1, 2, 3)))

    val combiner = Combine[Int, Int, Long]((rdd1, rdd2) => parallelize(List(rdd1.count(), rdd2.count())), pipeline1)

    val combinedPipeline = combiner(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(2, 3)
  }

  it should "accept same Pipeline with non empty RDD" in {
    val pipeline = new Pipeline(parallelize(List(1, 2, 3)))

    val combiner = Combine[Int, Int, Long]((rdd1, rdd2) => parallelize(List(rdd1.count(), rdd2.count())), pipeline)

    val combinedPipeline = combiner(pipeline)

    combinedPipeline.testCollect should contain theSameElementsAs List(3, 3)
  }
}
