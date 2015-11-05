package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

//TODO Should be a trait with several implementations
class LookupProvider(/*dataset: RDD[Any]*/) extends Serializable
{
  //TODO Actually do the lookup
  //private def lookupTable=dataset.collect()
  private def dummy:LabeledPoint=new LabeledPoint(1,new DenseVector(Array[Double](1.0, 1.0, 1.0, 1.0)))
  def lookup(index:Long):LabeledPoint=
  {
    return dummy
  }
}