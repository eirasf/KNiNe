package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext

trait LookupProvider extends Serializable
{
  def lookup(index:Long):LabeledPoint;
}

class DummyLookupProvider() extends LookupProvider
{
  //private def lookupTable=dataset.collect()
  private def dummy:LabeledPoint=new LabeledPoint(1,new DenseVector(Array[Double](1.0, 1.0, 1.0, 1.0)))
  def lookup(index:Long):LabeledPoint=
  {
    return dummy
  }
}

class BroadcastLookupProvider(dataset: RDD[(LabeledPoint, Long)]) extends LookupProvider
{
  /* Test to check the order of the collected items
  val test=dataset.sortBy(_._2).collect()
  for(x <- test)
    println(x)*/
  dataset.count().toInt //This should throw an exception if the dataset is too large
  
  val bData=sparkContextSingleton.getInstance().broadcast(dataset.sortBy(_._2).collect())
  
  def lookup(index:Long):LabeledPoint=
  {
    return bData.value(index.toInt)._1
  }
}