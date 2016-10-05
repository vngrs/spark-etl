package com.vngrs.etl

/**
  * [[com.vngrs.etl.Extract]] Specs
  */
class ExtractSpec extends BaseSparkSpec {

  "An Extractor" should "convert an empty RDD to an extractor" in {
    val rdd = emptyRDD[Int]

    Extract(rdd).testRun should be (rdd)
  }

  it should "convert any non empty RDD to an extractor" in {
    val rdd = parallelize(List(1, 2, 3))

    Extract(rdd).testRun should be (rdd)
  }

  it should "convert an empty iterable to an extractor" in {
    val iterable = List[Int]()

    val extractor = Extract(iterable)

    extractor.testCollect should contain theSameElementsAs iterable
  }

  it should "convert any non empty iterable to an extractor" in {
    val iterable = List(1, 2, 3)

    val extractor = Extract(iterable)

    extractor.testCollect should contain theSameElementsAs iterable
  }

  it should "create an Extractor by calling the given function which returns an empty RDD" in {
    val rdd = emptyRDD[Int]

    val extractor = Extract(sc => rdd)

    extractor.testRun should equal (rdd)
  }

  it should "create an Extractor by calling the given function which returns any non empty RDD" in {
    val data = List(1, 2, 3)
    val rdd = parallelize(data)

    val extractor = Extract(sc => rdd)

    extractor.testCollect should contain theSameElementsAs data
  }
}
