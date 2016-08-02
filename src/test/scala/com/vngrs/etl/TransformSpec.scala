package com.vngrs.etl

import org.apache.spark.rdd.RDD

/**
  * Transformer Specifications
  */
class TransformSpec extends SparkSpec {

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

  "Transformer factory" should "create Transformer with simple function" in {
    val rdd = emptyRDD[Int]

    val transformer = Transform[Int, Int](d => d)

    val transformedRdd = transformer(rdd)

    transformedRdd should equal (rdd)
  }

  it should "create Transformer with (a little bit) complex function" in {
    val rdd = parallelize(List(1, 2, 3))

    val transformer = Transform[Int, Int](d => d.map(i => Math.pow(i, 2)).map(_.toInt).filter(_ >= 4))

    val transformedRdd = transformer(rdd)

    transformedRdd.collect() should contain theSameElementsAs List(4, 9)
  }

  "A mapper Transformer" should "accept an Empty RDD" in {
    val rdd = emptyRDD[String]

    val transformer = Transform.map[String, String](_.toUpperCase())
    val transformedRdd = transformer(rdd)

    transformedRdd.collect() should contain theSameElementsAs List[String]()
  }

  it should "transform any non empty RDD" in {
    val list = List("a", "b", "c")
    val rdd = parallelize(list)

    val transformer = Transform.map[String, String](_.toUpperCase())
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
