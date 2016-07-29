package com.vngrs.etl

import com.vngrs.etl.combiners.{Unioner, Zipper}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Represents ETL Pipeline.
  *
  * @param rdd The [[org.apache.spark.rdd.RDD]] which will be ETLed
  * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
  */
final class Pipeline[A: ClassTag](val rdd: RDD[A]) {

  /**
    * Transforms [[com.vngrs.etl.Pipeline]] with given [[com.vngrs.etl.Transformer]].
    *
    * @param transformer [[com.vngrs.etl.Transformer]]
    * @tparam B Type of the output [[com.vngrs.etl.Pipeline]]
    * @return Transformed [[com.vngrs.etl.Pipeline]]
    */
  def transform[B: ClassTag](transformer: Transformer[A, B]): Pipeline[B] = Pipeline(transformer(rdd))

  /**
    * Loads this [[com.vngrs.etl.Pipeline]] to given [[com.vngrs.etl.Loader]](s).
    *
    * @param loaders[[com.vngrs.etl.Loader]]
    */
  def load(loaders: Loader[A]*): Unit = loaders.foreach(loader => loader(rdd))

  /**
    * Combines this [[com.vngrs.etl.Pipeline]] with another [[com.vngrs.etl.Pipeline]]
    * by given [[com.vngrs.etl.Combiner]]
    *
    * @param combiner [[com.vngrs.etl.Combiner]]
    * @tparam B Type of another [[com.vngrs.etl.Pipeline]]
    * @return Combined [[com.vngrs.etl.Pipeline]]
    */
  def combine[B](combiner: Combiner[A, B]): Pipeline[B] = combiner(this)

  /**
    * Zips this [[com.vngrs.etl.Pipeline]] with the `other` one.
    *
    * @param other [[com.vngrs.etl.Pipeline]]
    * @tparam B Type of the `other` [[com.vngrs.etl.Pipeline]]
    * @return Zipped [[com.vngrs.etl.Pipeline]]
    */
  def zip[B: ClassTag](other: Pipeline[B]): Pipeline[(A, B)] = combine(Zipper(other))

  /**
    * Unions this [[com.vngrs.etl.Pipeline]] with the `other` one.
    *
    * @param other [[com.vngrs.etl.Pipeline]]
    * @return Unioned [[com.vngrs.etl.Pipeline]]
    */
  def union(other: Pipeline[A]): Pipeline[A] = combine(Unioner(other))
}

/**
  * Companion object which acts as a Factory.
  */
// Since this is a factory object, overloading warning has b
// een suppressed
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object Pipeline {

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[org.apache.spark.rdd.RDD]].
    *
    * @param rdd [[com.vngrs.etl.Extractor]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](rdd: RDD[A]): Pipeline[A] = new Pipeline(rdd)

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[com.vngrs.etl.Extractor]].
    *
    * @param extractor [[com.vngrs.etl.Extractor]]
    * @param sc [[org.apache.spark.SparkContext]]
    * @tparam A Type of the [[org.apache.spark.rdd.RDD]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](extractor: Extractor[A])(implicit sc: SparkContext): Pipeline[A] = new Pipeline(extractor(sc))

  /**
    * Creates a [[com.vngrs.etl.Pipeline]] with given [[Iterable]].
    *
    * @param it [[com.vngrs.etl.Extractor]]
    * @param sc [[org.apache.spark.SparkContext]]
    * @tparam A Type of the [[Iterable]]
    * @return [[com.vngrs.etl.Pipeline]]
    */
  def apply[A: ClassTag](it: Iterable[A])(implicit sc: SparkContext): Pipeline[A] = Pipeline(Extractor(it))
}

private object UsageExamples {

  implicit val sc: SparkContext = new SparkContext()

  val extractor = new Extractor[String] {
    override def apply(sc: SparkContext): RDD[String] = sc.parallelize[String](Seq("A", "B", "C"))
  }

  val transformer = new Transformer[String, String] {
    override def apply(data: RDD[String]): RDD[String] = data.map(_.toLowerCase())
  }

  val squareTrans = new Transformer[Int, Double] {
    override def apply(data: RDD[Int]): RDD[Double] = data.map(Math.pow(2, _))
  }

  val loader = new Loader[String] {
    override def apply(data: RDD[String]): Unit = data.saveAsTextFile("path")
  }

  val extractorNum = new Extractor[Int] {
    override def apply(sc: SparkContext): RDD[Int] = sc.parallelize[Int](Seq(1, 2, 3))
  }

  val transformerNum = new Transformer[(String, Int), String] {
    override def apply(data: RDD[(String, Int)]): RDD[String] = data.map(t => s"${t._1}|${t._2}")
  }

  val loader2 = new Loader[String] {
    override def apply(data: RDD[String]): Unit = data.saveAsTextFile("path")
  }

  final case class PowTrans(pow: Double) extends Transformer[Int, Double] {
    override def apply(data: RDD[Int]): RDD[Double] = data.map(Math.pow(pow, _))
  }

  final case class FileLoader(path: String) extends Loader[String] {
    override def apply(data: RDD[String]): Unit = data.saveAsTextFile(path)
  }

  Pipeline(extractor)
    .transform(transformer)
    .load(loader)

  Pipeline(extractor)
    .zip(Pipeline(extractorNum))
    .transform(transformerNum)
    .load(loader)

  Pipeline(extractor)
    .zip(Pipeline(extractorNum))
    .transform(transformerNum)
    .load(loader, loader2)

  Pipeline(extractorNum)
    .transform(PowTrans(2.0d))
    .transform(Transformer.filter(_ > 15))
    .zip(
      Pipeline(extractorNum)
        .transform(squareTrans)
        .transform(Transformer.filter(_ > 3))
    )
    .transform(Transformer(_.toString))
    .load(FileLoader("path/to/path"))

  val pipeline1 = Pipeline(extractorNum)
  val pipeline2 = Pipeline(extractorNum)

  val zippedPipeline = pipeline1.combine(Zipper(pipeline2))
}