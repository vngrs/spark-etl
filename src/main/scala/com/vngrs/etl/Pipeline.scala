package com.vngrs.etl

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

final class Pipeline[A](val rdd: RDD[A]) extends AnyVal {
  def transform[B: ClassTag](transformer: Transformer[A, B]): Pipeline[B] = Pipeline(transformer(rdd))

  def load(loaders: Loader[A]*): Unit = loaders.foreach(loader => loader(rdd))

  def zip[B: ClassTag](other: Pipeline[B]): Pipeline[(A, B)] = new Pipeline(rdd.zip(other.rdd))

  def union(other: Pipeline[A]): Pipeline[A] = new Pipeline(rdd.union(other.rdd))
}

object Pipeline {
  def apply[A: ClassTag](extractor: Extractor[A])(implicit sc: SparkContext): Pipeline[A] = Pipeline(extractor(sc))

  def apply[A: ClassTag](iterable: Iterable[A])(implicit sc: SparkContext): Pipeline[A] = Pipeline(sc.parallelize[A](iterable.toSeq))

  def apply[A: ClassTag](arr: Array[A])(implicit sc: SparkContext): Pipeline[A] = Pipeline(sc.parallelize[A](arr))

  def apply[A: ClassTag](rdd: RDD[A]): Pipeline[A] = new Pipeline(rdd)
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

  val zippedPipeline = Aggregator.zip(pipeline1, pipeline2)
}