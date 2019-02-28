package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{DenseVector => BDV}

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
  
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val closestNeighbors=new Array[Array[NeighborsForElement]](arrayIndices.length) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
    
    //The computed distances could be stored elsewhere so that there is no symmetric repetition
    for(i <- 0 until arrayIndices.length)
    {
      closestNeighbors(i)=new Array[NeighborsForElement](grouper.numGroups)
      for(j <- 0 until grouper.numGroups)
        closestNeighbors(i)(j)=new NeighborsForElement(numNeighbors)
    }
    
    var graph:List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=Nil //Graph to be returned
    var counts:Array[BDV[Int]]=new Array[BDV[Int]](arrayIndices.length)
    for(i <- 0 until arrayIndices.length)
      counts(i)=BDV.zeros[Int](grouper.numGroups)
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
         
         counts(i)(grIdY)+=1
         counts(j)(grIdX)+=1
      }
      
      graph = (arrayIndices(i),
                (counts(i),
                 closestNeighbors(i).indices.map(
                     {
                       case groupingIndex => var neighbors:List[(Long, Double)]=Nil
                                             for (j <- closestNeighbors(i)(groupingIndex).listNeighbors)
                                               neighbors=(j.index, j.distance) :: neighbors
                                             (groupingIndex,neighbors.toSet.toList)
                     }).toList)
              ) :: graph
    }
    
    graph
  }
  
  def computeGroupedGraph(list1:Iterable[Long], list2:Iterable[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val points2=list2.map({case index =>
                            val p=lookup.lookup(index)
                            val closestNeighbors=new Array[NeighborsForElement](grouper.numGroups) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
                            for(j <- 0 until grouper.numGroups)
                              closestNeighbors(j)=new NeighborsForElement(numNeighbors)
                            (index,p,grouper.getGroupId(p),closestNeighbors,BDV.zeros[Int](grouper.numGroups))
                          })
    list1.map({case index =>
              val p=lookup.lookup(index)
              val grId=grouper.getGroupId(p)
              val closestNeighbors=new Array[NeighborsForElement](grouper.numGroups) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
              for(j <- 0 until grouper.numGroups)
                closestNeighbors(j)=new NeighborsForElement(numNeighbors)
              val counts=BDV.zeros[Int](grouper.numGroups)
              for ((i2,p2,grId2,neighList2,counts2) <- points2)
              {
                val d=measurer.getDistance(p,p2)
                var nList=closestNeighbors(grId2)
                nList.addElement(i2, d)
                neighList2(grId).addElement(index, d)
                counts(grId2)+=1
                counts2(grId)+=1
              }
              (index,(counts,closestNeighbors.indices.map(
                                                            {
                                                               case groupingIndex => (groupingIndex,closestNeighbors(groupingIndex).listNeighbors.map({case x => (x.index, x.distance)}).toSet.toList)
                                                             }).toList))
          }).toList ++ points2.map({case (id, p, grId, groupedNeighs, counts) => (id,(counts,groupedNeighs.indices.map(
                                                            {
                                                               case groupingIndex => (groupingIndex,groupedNeighs(groupingIndex).listNeighbors.map({case x => (x.index, x.distance)}).toSet.toList)
                                                             }).toList))})
  }
  
  def computeGroupedGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int):List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val arrayIndices=data.map(_._1).collect()
    val lookup=new BroadcastLookupProvider(data)
    computeGroupedGraph(arrayIndices, new BroadcastLookupProvider(data), numNeighbors)
  }
  
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int):List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, new EuclideanDistanceProvider())
    
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider):List[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, new DummyGroupingProvider())
  
  /*def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val graph=computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, grouper)
    return graph.map(
        {
          case (index, (numVisited, groupedNeighbors)) => (index, (numVisited,groupedNeighbors.head._2))
        })
  }*/
  
  def parallelComputeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider, numPartitions:Int):RDD[(Long, List[(Int,List[(Long, Double)])])]=
  {
    var sc=sparkContextSingleton.getInstance()
    
    var rddIndices=sc.parallelize(arrayIndices, numPartitions)
    
    var rddPairs=rddIndices.cartesian(rddIndices).coalesce(numPartitions)
    
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
  
  def parallelComputeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, numPartitions:Int):(RDD[(Long, List[(Long, Double)])],LookupProvider)=
    parallelComputeGraph(data, numNeighbors, new EuclideanDistanceProvider(), numPartitions)
    
  def parallelComputeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, measurer:DistanceProvider, numPartitions:Int):(RDD[(Long, List[(Long, Double)])],LookupProvider)=
  {
    val lookup=new BroadcastLookupProvider(data)
    val graph=parallelComputeGroupedGraph(data.map(_._1).collect(), lookup, numNeighbors, measurer, new DummyGroupingProvider(), numPartitions)
    return (graph.map(
               {case (i1,groupedNeighbors) =>
                 (i1,groupedNeighbors.head._2)
               }),lookup)
  }
}