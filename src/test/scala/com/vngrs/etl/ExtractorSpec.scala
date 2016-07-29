package com.vngrs.etl

/**
  * [[com.vngrs.etl.Extractor]] Specs
  */
class ExtractorSpec extends BaseSparkSpec {

  "An Extractor" should "convert any RDD to an extractor" in {
    val rdd = sc.emptyRDD[Int]

    Extractor(rdd).testRun should be (rdd)
  }

  it should "convert any iterable to an extractor" in {
    val iterable = List(1, 2, 3)

    val extractor = Extractor(iterable)

    extractor.testRun should contain theSameElementsAs iterable
  }
}
