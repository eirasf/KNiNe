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
  
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val closestNeighbors=new Array[Array[NeighborsForElement]](arrayIndices.length) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
    
    //The computed distances could be stored elsewhere so that there is no symmetric repetition
    for(i <- 0 until arrayIndices.length)
    {
      closestNeighbors(i)=new Array[NeighborsForElement](grouper.numGroups)
      for(j <- 0 until grouper.numGroups)
        closestNeighbors(i)(j)=new NeighborsForElement(numNeighbors)
    }
    
    var graph:List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=Nil //Graph to be returned
    for(i <- 0 until arrayIndices.length)
    {
      for(j <- i+1 until arrayIndices.length)
      {
         if (measurer==null)
           println("NULL measurer")
         if (lookup==null)
           println("NULL lookup")
         if (arrayIndices==null)
           println("NULL arrayIndices")
         val d=measurer.getDistance(lookup.lookup(arrayIndices(i)),
                                     lookup.lookup(arrayIndices(j)))
         
         //println("D("+arrayIndices(i)+"<->"+arrayIndices(j)+")="+d+"#"+feat1.toString()+feat2.toString())
         
         val x=lookup.lookup(arrayIndices(i))
         val y=lookup.lookup(arrayIndices(j))
         val grIdX=grouper.getGroupId(x)
         val grIdY=grouper.getGroupId(y)
         
         var xList=closestNeighbors(i)(grIdY)
         var yList=closestNeighbors(j)(grIdX)
                                     
         xList.addElement(arrayIndices(j), d)
         yList.addElement(arrayIndices(i), d)
      }
      
      /*var groupedNeighbors:List[(Int,List[(Long, Double)])]=Nil //Graph to be returned
      
      for ((grId,neighborList) <- closestNeighbors(i))
      {
        //Unwrap the structure into graph edges
        var neighbors:List[(Long, Double)]=Nil
        for (j <- neighborList.listNeighbors)
          neighbors=(j.index, j.distance) :: neighbors
        //TODO Add whatever more information is useful (distances, number of elements hit).
        groupedNeighbors = (grId, neighbors) :: groupedNeighbors
      }
      graph = (arrayIndices(i), (arrayIndices.length-1, groupedNeighbors)) :: graph*/
      
      graph = (arrayIndices(i),
                (arrayIndices.length-1,
                 closestNeighbors(i).indices.map(
                     {
                       case groupingIndex => var neighbors:List[(Long, Double)]=Nil
                                             for (j <- closestNeighbors(i)(groupingIndex).listNeighbors)
                                               neighbors=(j.index, j.distance) :: neighbors
                                             (groupingIndex,neighbors)
                     }).toList)
              ) :: graph
    }
    
    graph
  }
  
  def computeGroupedGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val arrayIndices=data.map(_._1).collect()
    val lookup=new BroadcastLookupProvider(data)
    computeGroupedGraph(arrayIndices, new BroadcastLookupProvider(data), numNeighbors)
  }
  
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, new EuclideanDistanceProvider())
    
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, new DummyGroupingProvider())
  
  /*def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val graph=computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, grouper)
    return graph.map(
        {
          case (index, (numVisited, groupedNeighbors)) => (index, (numVisited,groupedNeighbors.head._2))
        })
  }*/
  
  def parallelComputeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, List[(Int,List[(Long, Double)])])]=
  {
    var sc=sparkContextSingleton.getInstance()
    
    var rddIndices=sc.parallelize(arrayIndices)
    
    var rddPairs=rddIndices.cartesian(rddIndices)
    
    //TODO Check whether filtering i<j improves performance
    return rddPairs.flatMap({case (i,j) => if (i==j)
                                             None
                                           else
                                           {
                                             val x=lookup.lookup(i)
                                             val y=lookup.lookup(j)
                                             val d=measurer.getDistance(x,y)
                                             
                                             val n=new NeighborsForElement(numNeighbors)
                                             n.addElement(j, d)
                                             Some(((i,grouper.getGroupId(x)), n))
                                           }
                })
            .reduceByKey({case (n1, n2) => n1.addElements(n2)
                                           n1
                })
            .map({case ((index,groupingId), neighbors) => (index,
                                              List((groupingId,neighbors.listNeighbors.map { x => (x.index, x.distance) })))
                })
            .reduceByKey({case (l1,l2) => l1++l2})
  }
  
  def parallelComputeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int):(RDD[(Long, List[(Long, Double)])],LookupProvider)=
    parallelComputeGraph(data, numNeighbors, new EuclideanDistanceProvider())
    
  def parallelComputeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, measurer:DistanceProvider):(RDD[(Long, List[(Long, Double)])],LookupProvider)=
  {
    val lookup=new BroadcastLookupProvider(data)
    val graph=parallelComputeGroupedGraph(data.map(_._1).collect(), lookup, numNeighbors, measurer, new DummyGroupingProvider())
    return (graph.map(
               {case (i1,groupedNeighbors) =>
                 (i1,groupedNeighbors.head._2)
               }),lookup)
  }
}