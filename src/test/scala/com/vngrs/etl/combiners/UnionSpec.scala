package com.vngrs.etl.combiners

import com.vngrs.etl.{BaseSparkSpec, Pipeline}

/**
  * Union Combiner's Specifications
  */
class UnionSpec extends BaseSparkSpec {

  "A Union Combiner" should "accept two Pipeline with Empty RDDs" in {
    val pipeline1 = new Pipeline(emptyRDD[Int])
    val pipeline2 = new Pipeline(emptyRDD[Int])

    val unionCombiner = Union(pipeline1)

    val combinedPipeline = unionCombiner(pipeline2)

    combinedPipeline.testCollect.length should equal (0)
  }

  it should "union any two Pipeline with non empty RDDs" in {
    val pipeline1 = new Pipeline(parallelize(List(1, 2, 3)))
    val pipeline2 = new Pipeline(parallelize(List(3, 4, 5)))

    val unionCombiner = Union(pipeline1)

    val combinedPipeline = unionCombiner(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 3, 4, 5)
  }

  it should "union any two Pipeline with non empty RDDs when lengths are variable" in {
    val pipeline1 = new Pipeline(parallelize(List(1, 2, 3)))
    val pipeline2 = new Pipeline(parallelize(List(1, 2, 3, 4)))

    val unionCombiner = Union(pipeline1)

    val combinedPipeline = unionCombiner(pipeline2)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 1, 2, 3, 4)
  }

  it should "accept same Pipeline with non empty RDD to populate its elements" in {
    val pipeline = new Pipeline(parallelize(List(1, 2, 3)))

    val unionCombiner = Union(pipeline)

    val combinedPipeline = unionCombiner(pipeline)

    combinedPipeline.testCollect should contain theSameElementsAs List(1, 2, 3, 1, 2, 3)
  }
}
