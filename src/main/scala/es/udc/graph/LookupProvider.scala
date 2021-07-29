package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

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

class BroadcastLookupProvider(dataset: RDD[(Long,LabeledPoint)]) extends LookupProvider
{
  /* Test to check the order of the collected items
  val test=dataset.sortBy(_._2).collect()
  for(x <- test)
    println(x)*/
  dataset.count().toInt //This should throw an exception if the dataset is too large
  
  val bData=sparkContextSingleton.getInstance().broadcast(dataset.sortBy(_._1).map(_._2).collect())
  
  def lookup(index:Long):LabeledPoint=
  {
    return bData.value(index.toInt)
  }
}

class SplittedBroadcastLookupProvider(dataset: RDD[(Long,LabeledPoint)]) extends LookupProvider
{
  /* Test to check the order of the collected items
  val test=dataset.sortBy(_._2).collect()
  for(x <- test)
    println(x)*/
  val NUM_PARTS=4
  
  val numElems=dataset.count().toInt //This should throw an exception if the dataset is too large
  val numElemsPerPart=math.ceil(numElems.toDouble/NUM_PARTS).toInt
  
  val bDatas:Array[Broadcast[Array[LabeledPoint]]]=new Array[Broadcast[Array[LabeledPoint]]](NUM_PARTS)
  
  for (i <- 0 until NUM_PARTS)
  {
    bDatas(i)=sparkContextSingleton.getInstance().broadcast(dataset.filter({case (index,p) => (index>=i*numElemsPerPart) && (index<(i+1)*numElemsPerPart)}).sortBy(_._1).map(_._2).collect())
  }
  
  def lookup(index:Long):LabeledPoint=
  {
    assert(index>=0)
    var i=0
    while (index>=(i+1)*numElemsPerPart)
      i+=1
    return bDatas(i).value((index-i*numElemsPerPart).toInt)
  }
}