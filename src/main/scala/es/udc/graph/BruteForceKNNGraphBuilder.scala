package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

object BruteForceKNNGraphBuilder
{
  class IndexDistancePair(pIndex: Long, pDistance: Double)
  {
    var index: Long = pIndex
    var distance: Double = pDistance
  }
  
  class NeighborsForElement(pNumNeighbors:Int)
  {
    private var numNeighbors=pNumNeighbors
    
    var maxDistance=Double.MinValue
    var listNeighbors=List[IndexDistancePair]()
    
    def addElement(index:Long, distance:Double):Unit=
    {
      if (listNeighbors.size<numNeighbors)
         {
           if (maxDistance<distance)
             maxDistance=distance
           listNeighbors=new IndexDistancePair(index, distance) :: listNeighbors 
         }
         else //Already have enough neighbors
         {
           if (maxDistance>distance) //Only update if this one is closer than the ones we already have
           {
             //Loop once through the existing neighbors replacing the one that was farthest with the new one and, at the same time, updating the maximum distance
             var maxDistanceFound=Double.MinValue
             var isInserted=false
             for (n <- listNeighbors)
             {
               if (!isInserted)
               {
                 if (n.distance==maxDistance)
                 {
                   n.index=index
                   n.distance=distance
                   isInserted=true
                 }
               }
               if (n.distance>maxDistanceFound)
                 maxDistanceFound=n.distance
             }
             maxDistance=maxDistanceFound
           }
         }
    }
    def addElements(n:NeighborsForElement)=
    {
      for (p <- n.listNeighbors)
        this.addElement(p.index, p.distance)
    }
  }
  
  def computeGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int):List[(Long, List[(Long, Double)])]=
  {
    val closestNeighbors=new Array[NeighborsForElement](arrayIndices.length) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
    
    //The computed distances could be stored elsewhere so that there is no symmetric repetition
    for(i <- 0 until arrayIndices.length)
      closestNeighbors(i)=new NeighborsForElement(numNeighbors)
    
    var graph:List[(Long, List[(Long, Double)])]=Nil //Graph to be returned
    for(i <- 0 until arrayIndices.length)
    {
      for(j <- i+1 until arrayIndices.length)
      {
         val d=getDistance(lookup.lookup(arrayIndices(i)),
                           lookup.lookup(arrayIndices(j)))
         
         //println("D("+arrayIndices(i)+"<->"+arrayIndices(j)+")="+d+"#"+feat1.toString()+feat2.toString())
         
         closestNeighbors(i).addElement(arrayIndices(j), d)
         closestNeighbors(j).addElement(arrayIndices(i), d)
      }
      
      //Unwrap the structure into graph edges
      var neighbors:List[(Long, Double)]=Nil
      for (j <- closestNeighbors(i).listNeighbors)
        neighbors=(j.index, j.distance) :: neighbors
      //TODO Add whatever more information is useful (distances, number of elements hit).
      graph = (arrayIndices(i), neighbors) :: graph
    }
    
    graph
  }
  
  //TODO Different distances could be used
  def getDistance(p1:LabeledPoint, p2:LabeledPoint):Double=Vectors.sqdist(p1.features, p2.features)
  
  def computeGraph(data:RDD[(LabeledPoint, Long)], numNeighbors:Int):List[(Long, List[(Long, Double)])]=
  {
    computeGraph(data.map(_._2).collect(), new BroadcastLookupProvider(data), numNeighbors)
  }
  
  def parallelComputeGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    var sc=sparkContextSingleton.getInstance()
    
    var rddIndices=sc.parallelize(arrayIndices)
    
    var rddPairs=rddIndices.cartesian(rddIndices)
    
    return rddPairs.map({case (i,j) => val feat1=lookup.lookup(i).features
                                 val feat2=lookup.lookup(j).features
                                 //TODO Different distances could be used
                                 val d=Vectors.sqdist(feat1, feat2)
                                 
                                 val n=new NeighborsForElement(numNeighbors)
                                 n.addElement(j, d)
                                 (i, n)
                })
            .reduceByKey({case (n1, n2) => n1.addElements(n2)
                                           n1
                })
            .map({case (index, neighbors) => (index,
                                              neighbors.listNeighbors.map { x => (x.index, x.distance) })
                })
  }
  
  def parallelComputeGraph(data:RDD[(LabeledPoint, Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    parallelComputeGraph(data.map(_._2).collect(), new BroadcastLookupProvider(data), numNeighbors)
  }
}