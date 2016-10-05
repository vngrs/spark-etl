package com.vngrs.etl

/**
  * Loader specifications
  */
class LoadSpec extends BaseSparkSpec {

  "A Loader" should "load an empty RDD" in {
    val rdd = emptyRDD[String]
    val stream = new StringBuilder()

    val sbLoader = Load[String](_.collect().foreach(stream.append))

    sbLoader(rdd)

    stream.toString() should equal ("")
  }

  it should "load any non empty RDD" in {
    val rdd = parallelize(List("1", "2", "3"))
    val stream = new StringBuilder()

    val sbLoader = Load[String](_.collect().foreach(stream.append))

    sbLoader(rdd)

    stream.toString() should equal ("123")
  }
}
