package com.vngrs.etl

import org.apache.spark.rdd.RDD

/**
  * Transformer Specifications
  */
class TransformSpec extends BaseSparkSpec {

  "A functionless Transformer" should "accept an empty RDD" in {
    val rdd = emptyRDD[String]

    val transformer = new Transform[String, String]{
      override def apply(input: RDD[String]): RDD[String] = rdd
    }

    val transformedRdd = transformer(rdd)

    transformedRdd should equal (rdd)
  }

  it should "keep same any non empty RDD" in {
    val list = List("a", "b", "c")
    val rdd = parallelize(list)

    val transformer = new Transform[String, String]{
      override def apply(input: RDD[String]): RDD[String] = rdd
    }

    val transformedRdd = transformer(rdd)

    transformedRdd.collect() should contain theSameElementsAs list
  }

  "A mapper Transformer" should "accept an Empty RDD" in {
    val rdd = emptyRDD[String]

    val transformer = Transform[String, String](_.toUpperCase())
    val transformedRdd = transformer(rdd)

    transformedRdd.collect() should contain theSameElementsAs List[String]()
  }

  it should "transform any non empty RDD" in {
    val list = List("a", "b", "c")
    val rdd = parallelize(list)

    val transformer = Transform[String, String](_.toUpperCase())
    val transformedRdd = transformer(rdd)

    transformedRdd.collect() should contain theSameElementsAs List("A", "B", "C")
  }

  "A filterer Transformer"  should "accept an Empty RDD" in {
    val rdd = emptyRDD[Int]

    val filterer = Transform.filter[Int](_ < 4)
    val filteredRdd = filterer(rdd)

    filteredRdd.collect() should contain theSameElementsAs List[Int]()
  }

  it should "filter any non Empty RDD" in {
    val rdd = parallelize(List(1, 2, 3, 4))

    val filterer = Transform.filter[Int](_ < 4)
    val filteredRdd = filterer(rdd)

    filteredRdd.collect() should contain theSameElementsAs List[Int](1, 2, 3)
  }
}
