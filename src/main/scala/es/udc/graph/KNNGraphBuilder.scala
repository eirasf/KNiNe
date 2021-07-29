package es.udc.graph

import Array._
import collection.mutable.Map
import collection.mutable.HashMap
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.HashPartitioner
import java.io.File
import org.apache.spark.SparkContext

object GraphBuilder
{
  def mergeSubgraphs(g1:RDD[(Long, GroupedNeighborsForElementWithComparisonCount)], g2:RDD[(Long, GroupedNeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    if (g1==null) return g2
    if (g2==null) return g1
    return g1.union(g2).reduceByKey({case (groupedNeighbors1, groupedNeighbors2) =>
                                                      groupedNeighbors1.addElements(groupedNeighbors2)
                                                      groupedNeighbors1
                                                    })
  }

  def readFromFiles(prefix:String, sc:SparkContext):RDD[(Long, GroupedNeighborsForElement)]=
  {
    println(s"Reading files from $prefix")
    //Get list of files involved
    val dirPath=prefix.substring(0,prefix.lastIndexOf("/")+1)
    val d = new File(dirPath)
    val allFilesInDir=
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    assert(!allFilesInDir.isEmpty)
    val matchingFiles=allFilesInDir.filter(
        {case f =>
            val sf=f.toPath().toString()
            sf.length()>prefix.length() && sf.substring(0,prefix.length())==prefix &&
            sf.endsWith(".txt")
            })
    val pattern=""".*c(\d+).txt""".r
    val rdds=matchingFiles.map(
        {case f =>
           val fs=f.toPath().toString()
           val groupId=fs match {case pattern(grId) => grId.toInt
                                 case default => 0
                                 }
           sc.textFile(fs)
             .map({case line =>
                     val parts=line.substring(1,line.length()-1).split(",")
                     (parts(0).toLong,IndexDistancePair(parts(1).toLong,parts(2).toDouble))
                 })
             .groupByKey()
             .map({case (id, nList) =>
                         val neighs=new NeighborsForElement(nList.size)
                         neighs.addElements(nList.toList)
                         (id, (groupId, neighs))})
         })
     val fullRDD=if (rdds.size>1)
                   sc.union(rdds)
                 else
                   rdds(0)
     println(fullRDD.count())
     val numNeighbors=fullRDD.map({case (id,(grId,neighs)) => neighs.listNeighbors.size}).max
     val groupIdList=matchingFiles.map(
        {case f =>
           val fs=f.toPath().toString()
           fs match {case pattern(grId) => grId.toInt
                     case default => 0}
        })
     fullRDD.groupByKey()
            .map({case (id,groupedList) =>
                      val map=scala.collection.mutable.Map(groupedList.toSeq : _*)
                      (id, new GroupedNeighborsForElement(map,groupIdList,numNeighbors))})
  }
}

abstract class GraphBuilder
{
  /**
 * @param g
 * @return Resulting graph of searching for closer neighbors in the neighbors of the neighbors for each node
 */
  /*private */def refineGraph(data:RDD[(Long,LabeledPoint)], g:RDD[(Long, NeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, NeighborsForElementWithComparisonCount)]=
  {
    val graph=refineGroupedGraph(data, g.map({case (index, neighs) => (index,neighs.asGroupedWithCounts())}), numNeighbors, measurer, new DummyGroupingProvider())
    return graph.map({case (i1, groupedNeighbors) => (i1, groupedNeighbors.asInstanceOf[WrappedUngroupedNeighborsForElementWithComparisonCount].unwrap())})
  }

  /*private */def refineGroupedGraph(data:RDD[(Long,LabeledPoint)], g:RDD[(Long, GroupedNeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    var pairsWithNewNeighbors:RDD[(Long, Long)]=g.flatMap(
                                              {
                                                case (node, neighbors) =>
                                                  val groupedNeighborLists=neighbors.groupedNeighborLists
                                                  val allNeighbors=groupedNeighborLists.flatMap(_._2.listNeighbors.map(_.index))
                                                  groupedNeighborLists.flatMap(
                                                      {
                                                        case (grId,neighs) => neighs.listNeighbors.map({case pair => (pair.index,node)})
                                                      })
                                                      .map({case (dest,node) => (dest,(node,allNeighbors))})
                                              }).partitionBy(data.partitioner.getOrElse(new HashPartitioner(data.getNumPartitions)))
                                         .join(g)
                                         .flatMap({case (dest, ((node,neighbors), neighsGroupedNeighs)) => neighsGroupedNeighs.groupedNeighborLists.flatMap(
                                                                                                       {
                                                                                                         case (grId,neighsNeighs) =>
                                                                                                             neighsNeighs.listNeighbors.flatMap(
                                                                                                                 {case pair => if ((node!=pair.index) && (!neighbors.contains(pair.index)))
                                                                                                                      Some((node, pair.index))
                                                                                                                    else
                                                                                                                      None})
                                                                                                       })})
                                         .map({case (x,y) => if (x<y) (x,y) else (y,x)})
    pairsWithNewNeighbors=pairsWithNewNeighbors.groupByKey().flatMap({case (d, neighs) => neighs.toSet.toArray.map({case x => (d, x)})})
    val totalElements=data.count()
    val bfOps:Double=totalElements*(totalElements-1)/2.0
    val stepOps=pairsWithNewNeighbors.count()
    println("Refining step takes "+stepOps+" comparisons ("+(stepOps/bfOps)+" wrt bruteforce)")
    var subgraph=getGroupedGraphFromIndexPairs(data,
                                        pairsWithNewNeighbors,
                                        numNeighbors,
                                        measurer,
                                        grouper)
    return GraphBuilder.mergeSubgraphs(g, subgraph, numNeighbors, measurer)
  }
  protected def getGroupedGraphFromIndexPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, GroupedNeighborsForElementWithComparisonCount)]
}

case class IndexDistancePair(var index: Long, var distance: Double)

object NeighborsForElement
{
  def merge(n1:NeighborsForElement, n2:NeighborsForElement):NeighborsForElement=
  {
    val maxNeighbors=math.max(n1.numNeighbors, n2.numNeighbors)
    var sNeighbors1=n1.listNeighbors.sortBy(_.distance)
    var sNeighbors2=n2.listNeighbors.sortBy(_.distance)

    var finalNeighbors:List[IndexDistancePair]=Nil

    while(finalNeighbors.size<maxNeighbors && (!sNeighbors1.isEmpty || !sNeighbors2.isEmpty))
    {
      if (sNeighbors2.isEmpty || (!sNeighbors1.isEmpty && sNeighbors1.head.distance<sNeighbors2.head.distance))
      {
        if ((finalNeighbors==Nil) || !finalNeighbors.contains(sNeighbors1.head))
          finalNeighbors=sNeighbors1.head :: finalNeighbors
        sNeighbors1=sNeighbors1.tail
      }
      else
      {
        if ((finalNeighbors==Nil) || !finalNeighbors.contains(sNeighbors2.head))
          finalNeighbors=sNeighbors2.head :: finalNeighbors
        sNeighbors2=sNeighbors2.tail
      }
    }

    val newElem=new NeighborsForElement(maxNeighbors)
    newElem.setListNeighbors(finalNeighbors)
    return newElem
  }
}

class NeighborsForElement(val numNeighbors:Int) extends Serializable
{
  private var _maxDistance=Double.MinValue
  protected var _listNeighbors=List[IndexDistancePair]()
  def listNeighbors=_listNeighbors
  def setListNeighbors(l:List[IndexDistancePair])=
  {
    _listNeighbors=l
    _maxDistance=Double.MinValue
    for (n <- l)
      if (n.distance>_maxDistance)
               _maxDistance=n.distance
  }

  def addElement(index:Long, distance:Double):Unit=
  {
    if (listNeighbors.size<numNeighbors)
     {
       if (_maxDistance<distance)
         _maxDistance=distance
       if (!listNeighbors.map(_.index).contains(index))
         _listNeighbors=IndexDistancePair(index, distance) :: listNeighbors
     }
     else //Already have enough neighbors
     {
       if ((_maxDistance>distance) && !listNeighbors.map(_.index).contains(index)) //Only update if this one is closer than the ones we already have
       {
         //Loop once through the existing neighbors replacing the one that was farthest with the new one and, at the same time, updating the maximum distance
         var maxDistanceFound=Double.MinValue
         var isInserted=false
         for (n <- listNeighbors)
         {
           if (!isInserted)
           {
             if (n.distance==_maxDistance)
             {
               n.index=index
               n.distance=distance
               isInserted=true
             }
           }
           if (n.distance>maxDistanceFound)
             maxDistanceFound=n.distance
         }
         _maxDistance=maxDistanceFound
       }
     }
  }
  def addElements(n:List[IndexDistancePair]):Unit=
  {
    for (p <- n)
      this.addElement(p.index, p.distance)
  }
  def addElements(n:NeighborsForElement):Unit=addElements(n.listNeighbors)

  def wrapWithCount(pComparisons:Int):NeighborsForElementWithComparisonCount=
  {
    val newN=new NeighborsForElementWithComparisonCount(numNeighbors, pComparisons)
    newN._listNeighbors=_listNeighbors
    return newN
  }
}

class NeighborsForElementWithComparisonCount(pNumNeighbors:Int, pComparisons:Int) extends NeighborsForElement(pNumNeighbors)
{
  def this(pNumNeighbors:Int)=this(pNumNeighbors,0)

  private var _comparisons=pComparisons
  def comparisons=_comparisons
  def addElements(n:NeighborsForElementWithComparisonCount):Unit=
  {
    super.addElements(n)
    _comparisons+=n.comparisons
  }

  def asGroupedWithCounts()=
    WrappedUngroupedNeighborsForElementWithComparisonCount.wrap(this)
}

class GroupedNeighborsForElement(pNeighbors:Map[Int,NeighborsForElement], val groupIdList:Iterable[Int], val numNeighbors:Int) extends Serializable
{
  protected val neighbors=pNeighbors
  def groupedNeighborLists=neighbors.toList
  def neighborsOfGroup(i:Int)=neighbors.get(i)
  def numberOfGroupsWithElements()=neighbors.size
  def numberOfGroupsWithAtLeastKElements(k:Int):Int=neighbors.values.count(_.listNeighbors.size>=k)
  def getIdsOfIncompleteGroups():List[Int]=
  {
    groupIdList
           .filter({case grId =>
                   val n=neighbors.get(grId)
                   n.isEmpty || (n.get.listNeighbors.size<n.get.numNeighbors)
                })
           .toList
  }

  def addElements(n:GroupedNeighborsForElement):Unit=
  {
    for ((k,v) <- n.neighbors)
      addElementsOfGroup(k,v)
  }

  private def getOrCreateGroup(groupId:Int):NeighborsForElement=
  {
    val g=neighbors.get(groupId)
    if (g.isDefined) return g.get
    val newGroup=new NeighborsForElement(numNeighbors)
    neighbors(groupId)=newGroup
    return newGroup
  }

  def addElementsOfGroup(groupId:Int, n:NeighborsForElement):Unit=
  {

    getOrCreateGroup(groupId).addElements(n)
  }

  def addElementOfGroup(groupId:Int, index:Long, distance:Double):Unit=
  {
    getOrCreateGroup(groupId).addElement(index, distance)
  }

  def wrapWithCounts(pComparisons:BDV[Int]):GroupedNeighborsForElementWithComparisonCount=
  {
    return new GroupedNeighborsForElementWithComparisonCount(neighbors, groupIdList, numNeighbors, pComparisons)
  }
}

object GroupedNeighborsForElement
{
  def newEmpty(groupIdList:Iterable[Int], numNeighbors:Int)=new GroupedNeighborsForElement(new HashMap[Int,NeighborsForElement], groupIdList, numNeighbors)
}

class GroupedNeighborsForElementWithComparisonCount(pNeighbors:Map[Int,NeighborsForElement], groupIdList:Iterable[Int], numNeighbors:Int, pComparisons:BDV[Int]) extends GroupedNeighborsForElement(pNeighbors,groupIdList,numNeighbors)
{
  private var _comparisons=pComparisons
  def comparisons=_comparisons

  def neighborsWithComparisonCountOfGroup(grId:Int):Option[NeighborsForElementWithComparisonCount]=
  {
    val neighs=super.neighborsOfGroup(grId)
    if (neighs.isEmpty)
      return None
    return Some(neighs.get.wrapWithCount(comparisons(grId)))
  }

  def getIdsOfGroupsWithAtLeastKComparisons(k:Int):List[Int]=
    comparisons.valuesIterator.zipWithIndex.filter(_._1>=k).map(_._2).toList

  def getIdsOfGroupsWithLessThanKComparisons(k:Int):List[Int]=
    comparisons.valuesIterator.zipWithIndex.filter(_._1<k).map(_._2).toList

  def addElements(n:GroupedNeighborsForElementWithComparisonCount):Unit=
  {
    super.addElements(n)
    _comparisons+=n.comparisons
  }

  def addElementsOfGroup(groupId:Int, n:NeighborsForElement, pComparisons:Int):Unit=
  {
    super.addElementsOfGroup(groupId,n)
    _comparisons(groupId)+=pComparisons
  }

  override def addElementOfGroup(groupId:Int, index:Long, distance:Double):Unit=
  {
    super.addElementOfGroup(groupId,index,distance)
    _comparisons(groupId)+=1
  }
}

object GroupedNeighborsForElementWithComparisonCount
{
  def newEmpty(groupIdList:Iterable[Int], numNeighbors:Int):GroupedNeighborsForElementWithComparisonCount=
    return GroupedNeighborsForElement.newEmpty(groupIdList,numNeighbors).wrapWithCounts(BDV.zeros[Int](groupIdList.size))
}

object WrappedUngroupedNeighborsForElementWithComparisonCount
{
  val wrappingGrouper=new DummyGroupingProvider()
  def wrap(n:NeighborsForElementWithComparisonCount):WrappedUngroupedNeighborsForElementWithComparisonCount=
  {
    val newElem=new WrappedUngroupedNeighborsForElementWithComparisonCount(new HashMap[Int,NeighborsForElement],
                                                                            wrappingGrouper.getGroupIdList(),
                                                                            n.numNeighbors,
                                                                            BDV.zeros[Int](wrappingGrouper.numGroups))
    newElem.addElementsOfGroup(wrappingGrouper.DEFAULT_GROUPID, n, n.comparisons)
    return newElem
  }
}

class WrappedUngroupedNeighborsForElementWithComparisonCount(pNeighbors:Map[Int,NeighborsForElement], groupIdList:Iterable[Int], numNeighbors:Int, pComparisons:BDV[Int]) extends GroupedNeighborsForElementWithComparisonCount(pNeighbors,groupIdList,numNeighbors,pComparisons)
{
  def unwrap():NeighborsForElementWithComparisonCount=
  {
    return this.neighborsWithComparisonCountOfGroup(WrappedUngroupedNeighborsForElementWithComparisonCount.wrappingGrouper.DEFAULT_GROUPID).get
  }
}
