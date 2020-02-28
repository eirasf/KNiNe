package es.udc.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils
import org.apache.spark.HashPartitioner
import breeze.linalg.{DenseVector => BDV}

object GraphBuilder
{
  def mergeSubgraphs(g1:RDD[(Long, (BDV[Int], List[(Int,List[(Long, Double)])]))], g2:RDD[(Long, (BDV[Int], List[(Int,List[(Long, Double)])]))], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, (BDV[Int], List[(Int,List[(Long, Double)])]))]=
  {
    if (g1==null) return g2
    if (g2==null) return g1
    return g1.union(g2).reduceByKey({case ((viewed1,groupedNeighbors1), (viewed2,groupedNeighbors2)) =>
                                                      (viewed1+viewed2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                                                    })
  }
}

abstract class GraphBuilder
{
  /**
 * @param g
 * @return Resulting graph of searching for closer neighbors in the neighbors of the neighbors for each node
 */
  /*private */def refineGraph(data:RDD[(Long,LabeledPoint)], g:RDD[(Long, (BDV[Int],List[(Long, Double)]))], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, (BDV[Int],List[(Long, Double)]))]=
  {
    val grouper=new DummyGroupingProvider()
    val grId=grouper.getGroupId(data.first()._2)
    val graph=refineGroupedGraph(data, g.map({case (index, (comps,neighs)) => (index,(comps,List((grId,neighs))))}), numNeighbors, measurer, grouper)
    return graph.map({case (i1, (comps1,groupedNeighbors)) => (i1, (comps1,groupedNeighbors.head._2))})
  }
  
  /*private */def refineGroupedGraph(data:RDD[(Long,LabeledPoint)], g:RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    var pairsWithNewNeighbors:RDD[(Long, Long)]=g.flatMap(
                                              {
                                                case (node, (viewed,groupedNeighbors)) =>
                                                  val allNeighbors=groupedNeighbors.flatMap(_._2.map(_._1))
                                                  groupedNeighbors.flatMap(
                                                      {
                                                        case (grId,neighbors) => neighbors.map({case (dest, dist) => (dest,node)})
                                                      })
                                                      .map({case (dest,node) => (dest,(node,allNeighbors))})
                                              }).partitionBy(data.partitioner.getOrElse(new HashPartitioner(data.getNumPartitions)))
                                         .join(g)
                                         .flatMap({case (dest, ((node,neighbors), (v,neighsGroupedNeighs))) => neighsGroupedNeighs.flatMap(
                                                                                                       {
                                                                                                         case (grId,neighsNeighs) =>
                                                                                                             neighsNeighs.flatMap(
                                                                                                                 {case (d, dist) => if ((node!=d) && (!neighbors.contains(d)))
                                                                                                                      Some((node, d))
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
  protected def getGroupedGraphFromIndexPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]
}

object GraphMerger extends Serializable
{
  def mergeNeighborLists(neighbors1:List[(Long, Double)], neighbors2:List[(Long, Double)], numNeighbors:Int):List[(Long, Double)]=
      {
        var sNeighbors1=neighbors1.sortBy(_._2)
         var sNeighbors2=neighbors2.sortBy(_._2)
         
         var finalNeighbors:List[(Long, Double)]=Nil
         
         while(finalNeighbors.size<numNeighbors && (!sNeighbors1.isEmpty || !sNeighbors2.isEmpty))
         {
           if (sNeighbors2.isEmpty || (!sNeighbors1.isEmpty && sNeighbors1.head._2<sNeighbors2.head._2))
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
         finalNeighbors
      }
  
  def mergeGroupedNeighbors(groupedNeighbors1:List[(Int,List[(Long,Double)])], groupedNeighbors2:List[(Int,List[(Long,Double)])], numNeighbors:Int):List[(Int, List[(Long, Double)])]=
  {
    val src=if (!groupedNeighbors1.isEmpty) groupedNeighbors1 else groupedNeighbors2
    val dest=if (!groupedNeighbors1.isEmpty) groupedNeighbors2 else groupedNeighbors1
    
    val mapNeighbors2=dest.toMap
    src.map({case (grId1, l1) => 
                              val newList:List[(Long,Double)]=(if (mapNeighbors2.contains(grId1))
                                            l1 ++ mapNeighbors2.get(grId1).get
                                          else
                                            l1).toSet.toList
                              if (newList.size<=numNeighbors)
                                (grId1, newList)
                              else
                                (grId1, newList.sortBy(_._2).take(numNeighbors))
                                })
  }
}

object LSHKNNGraphBuilder
{
  val DEFAULT_RADIUS_START=0.1
}

abstract class LSHKNNGraphBuilder extends GraphBuilder
{
  final def computeGroupedGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, hasher:Hasher, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, List[(Int,List[(Long, Double)])])]=
  {
    var fullGraph:RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=null //(node, (viewed, List[(groupingId,List[(neighbor,distance))]]))
    var currentData:RDD[(Long,(LabeledPoint,Iterable[Int]))]=data.map({case (id, point) => (id,(point,List[Int]()))}) //Adding "special requests" list
    var radius=startRadius.getOrElse(LSHKNNGraphBuilder.DEFAULT_RADIUS_START)//5//0.1
    val totalElements=currentData.count()
    val bfOps:Double=totalElements*(totalElements-1)/2.0
    var totalOps:Long=0
    var numBuckets:Long=2
    var allElementsInSingleBucket=false
    var nodesLeft=currentData.count()
    val bHasher=data.sparkContext.broadcast(hasher)
    
    println(f"Starting $numNeighbors%d-NN graph computation for $nodesLeft%d nodes")
    println(f"\t R0=$radius%g")
    println(f"\t cMAX=${maxComparisonsPerItem.getOrElse("auto")}%s\n")
    //while(!currentData.isEmpty())
    //while(nodesLeft>numNeighbors)
    //while((numBuckets>1 || nodesLeft>numNeighbors) && nodesLeft>1)
    while(nodesLeft>numNeighbors && nodesLeft>1 && !allElementsInSingleBucket)
    {
      //Maps each element to numTables (hash, index) pairs with hashes of keyLength length.
      val hashRDD=(if (grouper.numGroups==1) //Special case for single group so that the number of hashes is not doubled and no useless separable buckets are created.
                    currentData.flatMap({case (index, (point, specialRequests)) =>
                                                    val hashes=bHasher.value.getHashes(point.features, index, radius)
                                                    val grId=grouper.getGroupId(point)
                                                    hashes.map({case (h,id) => (h.concat(new Hash(Array[Integer](-1))),(id,grId,false))})
                                                  })
                  else
                    currentData.flatMap({case (index, (point, specialRequests)) =>
                                                    val hashes=bHasher.value.getHashes(point.features, index, radius)
                                                    val grId=grouper.getGroupId(point)
                                                    if (specialRequests.isEmpty)
                                                      hashes.flatMap({case (h,id) => List[(Hash,(Long,Int,Boolean))]((h.concat(new Hash(Array[Integer](-1))),(id,grId,false)),(h.concat(new Hash(Array[Integer](grouper.getGroupId(point)))),(id,grId,false)))})
                                                    else
                                                      hashes.flatMap({case (h,id) => specialRequests.map({case request => (h.concat(new Hash(Array[Integer](request))),(id,grId,request==grId))}) ++ List[(Hash,(Long,Int,Boolean))]((h.concat(new Hash(Array[Integer](grouper.getGroupId(point)))),(id,grId,false)))})
                                                  })
                  ).coalesce(data.getNumPartitions)
      
      //TODO Should all distances be computed? Maybe there's no point in computing them if we still don't have enough neighbors for an example
      //Should they be stored/cached? It may be enough to store a boolean that records if they have been computed. LRU Cache?
      
      //Groups elements mapped to the same hash
      var hashBuckets:RDD[(Hash, Iterable[Long], Int)]=hashRDD.filter({case (h,(id,grId,searchesForSelfClass)) => h.values(h.values.size-1)<0})
                                                                .map({case (h,(id,grId,searchesForSelfClass)) => (h,id)})
                                                                .groupByKey()
                                                                .map({case (k, l) => (k, l.toSet)})
                                                                .flatMap({case (k, s) => if (s.size>1) Some((k, s, s.size)) else None})
      hashBuckets=hashBuckets.coalesce(data.getNumPartitions)
      val hashBucketsNotEmpty=(!hashBuckets.isEmpty())
      if (hashBucketsNotEmpty)
      {
        numBuckets=hashBuckets.count()
        val stepOps=hashBuckets.map({case (h,s,n) => (n,1)})
                       .reduceByKey(_+_)
  
        val numStepOps=stepOps.map({case x => x._2 * x._1 * (x._1 - 1) /2.0}).sum().toLong
        val largestBucketSize=stepOps.map(_._1).max
        allElementsInSingleBucket=largestBucketSize==nodesLeft
        totalOps=totalOps+numStepOps
  
        println(f"Performing $numStepOps%g ops (largest bucket has $largestBucketSize%d elements)")
        //println(hashBuckets.reduce({case ((h1,s1,n1),(h2,s2,n2)) => if (n1>n2) (h1,s1,n1) else (h2,s2,n2)})._1.values.map(_.toString()).mkString("|"))
        /*DEBUG
        stepOps.sortBy(_._1)
                       .foreach({case x => println(x._2+" buckets with "+x._1+" elements => "+(x._2 * x._1 * (x._1-1)/2.0)+" ops")})*/

        //println("Changed "+prevStepOps+"ops to "+postStepOps+"ops ("+(postStepOps/prevStepOps)+")")
        
        
        //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.
        
        var subgraph=getGroupedGraphFromBuckets(data, hashBuckets, numNeighbors, measurer, grouper).coalesce(data.getNumPartitions)
        fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer).coalesce(data.getNumPartitions)
      }
      else //DEBUG
        println("No hash buckets created")
      
      //Separable buckets
      //println((currentData.first()._1,currentData.first()._2._2))
      //val interestID=currentData.first()._1
      //Groups elements mapped to the same hash
      var hashSeparableBuckets:RDD[(Hash, Iterable[Long], Iterable[Long])]=hashRDD.filter({case (h,(id,grId,searchesForSelfClass)) => h.values(h.values.size-1)>=0})
                                                                          .map({case (h,(id,grId,searchesForSelfClass)) =>
                                                                                    val requestedId=h.values(h.values.size-1)
                                                                                    val leftPart=if ((requestedId!=grId) || searchesForSelfClass) List[Long](id) else List[Long]()
                                                                                    val rightPart=if (requestedId==grId) List[Long](id) else List[Long]()
                                                                                    (h,(leftPart,rightPart))})
                                                                          .reduceByKey({case ((y1,n1),(y2,n2)) => (y1++y2,n1++n2)})
                                                                          .map({case (k, (y, n)) => (k, (y.toSet, n.toSet))})
                                                                          .flatMap({case (k, (y, n)) =>
                                                                            if ((y.size>0) && (n.size>0))
                                                                            {
                                                                              //if (y.contains(interestID) || n.contains(interestID))
                                                                              //  println("Kept "+k.values(k.values.size-1)+" ["+y.map(_.toString).mkString(",")+"]"+"["+n.map(_.toString).mkString(",")+"]")
                                                                                Some((k, y, n))
                                                                            }
                                                                            else
                                                                            {
                                                                              //if (y.contains(interestID) || n.contains(interestID))
                                                                              //  println("Removed "+k.values(k.values.size-1)+" ["+y.map(_.toString).mkString(",")+"]"+"["+n.map(_.toString).mkString(",")+"]")
                                                                              None
                                                                            }})
      val hashSeparableBucketsNotEmpty=(!hashSeparableBuckets.isEmpty())                                                                 
      if (hashSeparableBucketsNotEmpty)
      {
        numBuckets=hashSeparableBuckets.count()
        val stepOps=hashSeparableBuckets.map({case (h,y,n) => (y.size*n.size,1)})
                                             .reduceByKey(_+_)
  
        val numStepOps=stepOps.map({case x => x._2 * x._1 }).sum().toLong
        val largestBucketSize=stepOps.map(_._1).max
        totalOps=totalOps+numStepOps
  
        println(f"Performing $numStepOps%g ops (largest separable bucket needs $largestBucketSize%d ops)")
        //println(hashBuckets.reduce({case ((h1,s1,n1),(h2,s2,n2)) => if (n1>n2) (h1,s1,n1) else (h2,s2,n2)})._1.values.map(_.toString()).mkString("|"))
        /*DEBUG
        stepOps.sortBy(_._1)
                       .foreach({case x => println(x._2+" buckets with "+x._1+" elements => "+(x._2 * x._1 * (x._1-1)/2.0)+" ops")})*/

        //println("Changed "+prevStepOps+"ops to "+postStepOps+"ops ("+(postStepOps/prevStepOps)+")")
        
        
        //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.
        
        var subgraph=getGroupedGraphFromPartitionedBuckets(data, hashSeparableBuckets, numNeighbors, measurer, grouper).coalesce(data.getNumPartitions)
        //println(f"Obtained ${subgraph.count()} nodes with links")
        
        //DEBUG
        /*val retrieved=subgraph.take(5)
        retrieved.foreach(println)
        println("----------------")
        for (i<-retrieved.map(_._1))
          fullGraph.filter(_._1==i).foreach(println)
        */
        /*val ofInterest=List[Long](subgraph.first()._1) ++ subgraph.first()._2._2.flatMap(_._2.map(_._1).toSet.toList)
        println(subgraph.first())
        println("################")
        val firstID=subgraph.first()._1
        currentData.filter(_._1==firstID).map({case (id,(p,r)) => (id,grouper.getGroupId(p),r)}).foreach(println)
        println("||||||||||||||||")
        for (i<-ofInterest)
        {
          currentData.filter(_._1==i).map({case (id,(p,r)) => (id,grouper.getGroupId(p),r)}).foreach(println)
          subgraph.filter(_._1==i).foreach(println)
        }
        println("----------------")
        for (i<-ofInterest)
          fullGraph.filter(_._1==i).foreach(println)*/
        fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer).coalesce(data.getNumPartitions)
        /*println("----------------")
        for (i<-retrieved.map(_._1))
          fullGraph.filter(_._1==i).foreach(println)
        for (i<-ofInterest)
          fullGraph.filter(_._1==i).foreach(println)*/
      }
      else //DEBUG
        println("No separable hash buckets created")
      
      if (hashBucketsNotEmpty || hashSeparableBucketsNotEmpty)
      {
        //Simplify dataset
        val newData=simplifyDataset(currentData, fullGraph, numNeighbors, maxComparisonsPerItem, grouper)
        currentData.unpersist(false)
        currentData=newData.coalesce(data.getNumPartitions)
        
        currentData.cache()
      }
      
      //DEBUG
      /*println("$$$$$$$$$$$$$$$$$$$$$$$$")
      currentData.flatMap({case (id,(p,r)) =>
                              val grId=grouper.getGroupId(p)
                              r.map({case req => ((grId,req),1)})})
                .reduceByKey(_+_)
                .sortBy(_._2)
                .foreach(println)
      */
      
      //Remove elements that need a grId that is no longer in the data.
      val existingGrIds=currentData.map({case (id,(p,r)) => grouper.getGroupId(p)}).distinct.collect()
      currentData=currentData.map({case (id,(p,r)) =>
                                      val numElems=r.size
                                      (id,(p,r.filter(existingGrIds.contains(_)),numElems))})
                              .filter({case (id,(p,r,c)) => (r.size>0) || (c==0)})
                              .map({case (id,(p,r,c)) => (id,(p,r))})
      
      //else
      //  radius*=2
      radius*=2
      nodesLeft=currentData.count()
      
      //DEBUG
      println(" ----- "+nodesLeft+" nodes left ("+currentData.filter({case (id,(p,r)) => !r.isEmpty}).count()+" with at least one group complete). Radius:"+radius)
      //currentData.map({case (id,(p,r)) => (r.size,1)}).reduceByKey(_+_).foreach(println)
      //fullGraph.foreach(println(_))
    }
    if (fullGraph!=null)
    {
      val incomplete=fullGraph.filter({case (id,(viewed,groups)) => (groups.size<grouper.numGroups) || !groups.forall({case (grId,neighborList) => neighborList.size>=numNeighbors})})
                              .map({case (id,(viewed,groups)) =>
                                val mapGroups=groups.toMap
                                val incompleteGroups=grouper.getGroupIdList().flatMap({case grId => if (!mapGroups.contains(grId) || (mapGroups.get(grId).get.size<numNeighbors))
                                                                                                  Some(grId)
                                                                                                else
                                                                                                  None})
                                (id,incompleteGroups)
                                  })
      if (!incomplete.isEmpty())
      {
        //DEBUG
        //incomplete.map({case (id,(viewed,groupSizes)) => (groupSizes.sum,1)}).reduceByKey(_+_).sortBy(_._1).foreach(println)
        //incomplete.take(100).foreach({case (id,(viewed,neighborCounts)) => println(id+" -> "+viewed+" views ("+neighborCounts.mkString(";")+")")})
        println("Recovering "+incomplete.count()+" nodes that didn't have all neighbors")
        currentData=currentData.union(incomplete.join(data).map({case (id,(requests,p)) => (id,(p,requests))})).coalesce(data.getNumPartitions)
      }
    }
    if (nodesLeft>0)
    {
      //println("Elements left:"+currentData.map(_._1).collect().mkString(", "))
      if (fullGraph!=null)
      {
        /*
        //If there are any items left, look in the neighbor's neighbors.
        val neighbors:RDD[Iterable[Long]]=fullGraph.cartesian(currentData)
                                                   .flatMap(
                                                       {
                                                         case ((dest, (viewed,groupedNeig)), (orig,point)) =>
                                                             groupedNeig.flatMap(
                                                                 {
                                                                   case (grId,neig) =>
                                                                     if (neig.map(_._1).contains(orig))
                                                                        Some((orig,(dest :: neig.map(_._1)).toSet))
                                                                      else
                                                                        None
                                                                 }
                                                                 )
                                                        })
                                                   .reduceByKey({case (dl1,dl2) => dl1 ++ dl2})
                                                   .map({case (o,dl) => dl + o})                                                   
        */
        //The remaining points are grouped and collected.
        val remainingData=currentData.map({case (id,(point,requests)) => (grouper.getGroupId(point),List((id,requests)))}).reduceByKey(_++_).collect()
        val bRemainingData=sparkContextSingleton.getInstance().broadcast(remainingData)
        //Get all nodes pointing to a remaining point
        val reverseNeighborPairs:RDD[(Long,Long)]=fullGraph.flatMap(
                                                       {
                                                         case (dest, (viewed,groupedNeig)) =>
                                                             val rData=bRemainingData.value
                                                             val rDataMap=rData.toMap
                                                             val groupedNeigMap=groupedNeig.toMap
                                                             //Each neighbor group that was already in the graph is examined
                                                             groupedNeig.flatMap(
                                                                 {
                                                                   case (grId,neig) =>
                                                                     //If we have remaining datapoints of that group
                                                                     if (rDataMap.contains(grId))
                                                                       rDataMap(grId).flatMap(
                                                                           {
                                                                             //Each datapoint of that group
                                                                             case (idOrig,requestsOrig) =>
                                                                                 //If it is one of the neighbors for dest in the graph
                                                                                 if (neig.map(_._1).contains(idOrig))
                                                                                 {
                                                                                   val srcList=if (requestsOrig.isEmpty) //Any neighbor will do, so all are selected
                                                                                                 grouper.getGroupIdList()
                                                                                               else
                                                                                                 requestsOrig
                                                                                   srcList.map({case reqId => (idOrig,(dest :: groupedNeigMap(reqId).map(_._1).filter(_!=idOrig)).toSet)})
                                                                                 }
                                                                                 else
                                                                                    None
                                                                             })
                                                                     else
                                                                       None
                                                                 }
                                                                 )
                                                        })
                                                   .reduceByKey({case (dl1,dl2) => dl1 ++ dl2})
                                                   .flatMap({case (o,dl) => dl.map((o,_))})
        
        val pairsWithNewNeighbors:RDD[(Long, Long)]=currentData
                                          .join(fullGraph) //Joined with currentData to restrict this to only elements in the current dataset.
                                          .flatMap({case (id,(point,(viewed,groupedNeighs))) => groupedNeighs.flatMap({case (grId,neighs) => neighs.flatMap({case (dest,dist) => Some((dest,id))})})})
                                          .groupByKey()
                                          .join(fullGraph)
                                          .flatMap(
                                              {
                                                case (via,(ids,(viewed,groupedNeighs))) =>
                                                  groupedNeighs.flatMap({case (grId,neighs) => neighs.flatMap({case (dest,dist) => ids.flatMap({case id=>if (id!=dest) Some((id,dest)) else None})})})
                                              })
        val totalPairs=pairsWithNewNeighbors
                                 .union(reverseNeighborPairs)
                                 .map({case (x,y) => if (x<y) (x,y) else (y,x)})
                                 .groupByKey().flatMap({case (d, neighs) => neighs.toSet.toArray.map({case x => (d, x)})})
        val newOps=totalPairs.count()
        println("Performing "+newOps+" additional comparisons")
        totalOps=totalOps+newOps.toLong
        var subgraph=getGroupedGraphFromIndexPairs(data,
                                            totalPairs,
                                            numNeighbors,
                                            measurer,
                                            grouper)
                                            
        if (!subgraph.isEmpty())
        {
//subgraph.foreach(println(_))
          fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer)
          currentData=simplifyDataset(currentData, fullGraph, numNeighbors, maxComparisonsPerItem, grouper)
          nodesLeft=currentData.count()
        }
        //println(nodesLeft+" nodes left after first attempt")
      }
      /*if (nodesLeft>0) //No solution other than to check these points with every other
      {
        val pairs=currentData.cartesian(fullGraph.map({case (point, neighbors) => point}))
        val subgraph=getGroupedGraphFromPairs(data, pairs, numNeighbors, measurer, grouper)
        fullGraph=mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer)
totalOps=totalOps+pairs.count()
      }*/
    }
    
    println(s"Operations wrt bruteforce: ${totalOps/bfOps} "+f"($totalOps%d total ops / ${bfOps.toLong}%d)")
    //println((totalOps/bfOps)+"#")
    
    return fullGraph.map({case (node, (viewed, neighs)) => (node,neighs)}).coalesce(data.getNumPartitions)
  }
  
  def computeGroupedGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, List[(Int,List[(Long, Double)])])]=
  {
    val cMax=if (maxComparisonsPerItem.isDefined) math.max(maxComparisonsPerItem.get,numNeighbors) else math.max(128,10*numNeighbors)
    val factor=2.0
    val (hasher,nComps,suggestedRadius)=EuclideanLSHasher.getHasherForDataset(data, (factor*cMax).toInt)
    return computeGroupedGraph(data, numNeighbors, hasher, Some(startRadius.getOrElse(suggestedRadius)), Some(cMax.toInt), measurer, grouper)
  }
  
  def computeGroupedGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, keyLength:Int, numTables:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, List[(Int,List[(Long, Double)])])]=
    computeGroupedGraph(data, numNeighbors, new EuclideanLSHasher(data.map({case (index, point) => point.features.size}).max(), keyLength, numTables), startRadius, maxComparisonsPerItem, measurer, grouper)
  
  def computeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]=
  {
    val cMax=if (maxComparisonsPerItem.isDefined) math.max(maxComparisonsPerItem.get,numNeighbors) else math.max(128,10*numNeighbors)
    val factor=2.0
    val (hasher,nComps,suggestedRadius)=EuclideanLSHasher.getHasherForDataset(data, (factor*cMax).toInt)
    return computeGraph(data, numNeighbors, hasher, Some(startRadius.getOrElse(suggestedRadius)), Some(cMax.toInt), measurer)
  }
  
  def computeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, hasher:Hasher, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]=
  {
    val graph=computeGroupedGraph(data, numNeighbors, hasher, startRadius, maxComparisonsPerItem, measurer, new DummyGroupingProvider())
    return graph.map(
        {
          case (index, groupedNeighs) => (index, groupedNeighs.head._2)
        })
  }
  
  def computeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, hasherKeyLength:Int, hasherNumTables:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]
            =computeGraph(data,
                           numNeighbors,
                           new EuclideanLSHasher(data.map({case (index, point) => point.features.size}).max(), hasherKeyLength, hasherNumTables),//Get dimension from dataset
                           startRadius,
                           maxComparisonsPerItem,
                           measurer)
                                                                                           
  protected def splitLargeBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], maxBucketSize:Int, radius:Double, hasher:Hasher):RDD[(Hash, Iterable[Long], Int)]
  
  protected def getGroupedGraphFromBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]
  
  protected def getGroupedGraphFromPartitionedBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Iterable[Long])], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]
  
  protected def getGroupedGraphFromElementIndexLists(data:RDD[(Long,LabeledPoint)], elementIndexLists:RDD[Iterable[Long]], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]
  
  protected def getGroupedGraphFromPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[((Long, LabeledPoint), Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (Int,List[(Int,List[(Long, Double)])]))]
  
  //protected def getGroupedGraphFromIndexPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]
  
  private def simplifyDataset(dataset:RDD[(Long,(LabeledPoint,Iterable[Int]))], currentGraph:RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))], numNeighbors:Int, maxComparisonsPerItem:Option[Int], grouper:GroupingProvider):RDD[(Long, (LabeledPoint, Iterable[Int]))]=
  {
    val requestsByNodes:RDD[(Long,Option[Iterable[Int]])]=if (maxComparisonsPerItem.isDefined)
                                                        currentGraph.map({case (index, (viewed,groupedList)) =>
                                                                            def isFull(x:Int):Boolean=x>maxComparisonsPerItem.get
                                                                            if (!viewed.forall(!isFull(_))) //There's at least one full
                                                                            {
                                                                              val mapGroups=groupedList.toMap
                                                                              val incompleteGroups=grouper.getGroupIdList().flatMap({case grId =>
                                                                                                                   if (!mapGroups.contains(grId) || (viewed(grId)<maxComparisonsPerItem.get))
                                                                                                                     Some(grId)
                                                                                                                   else
                                                                                                                     None
                                                                                                                     })
                                                                              (index,if (incompleteGroups.isEmpty) None else Some(incompleteGroups)) //None indicates that it should be removed.
                                                                            }
                                                                            else
                                                                              (index,Some(List[Int]()))
                                                                          })
                                                            
                                                      else//Remove only elements that already have all their neighbors
                                                        currentGraph.map({case (index, (viewed,groupedList)) =>
                                                                              val mapGroups=groupedList.toMap
                                                                              val incompleteGroups=grouper.getGroupIdList().flatMap({case grId =>
                                                                                                                   if (!mapGroups.contains(grId) || mapGroups.get(grId).get.size<numNeighbors)
                                                                                                                     Some(grId)
                                                                                                                   else
                                                                                                                     None
                                                                                                                     })
                                                                              (index,if (incompleteGroups.isEmpty) None else Some(incompleteGroups)) //None indicates that it should be removed.
                                                                          })
    return dataset.leftOuterJoin(requestsByNodes).flatMap({case (index, (neighbors1, requestsOption)) =>
                                                                if (!requestsOption.isDefined)
                                                                  Some((index, (neighbors1._1, List[Int]())))
                                                                else
                                                                {
                                                                  val requests=requestsOption.get
                                                                  if (requests.isDefined)
                                                                    Some((index, (neighbors1._1, requests.get)))
                                                                  else
                                                                    None
                                                                }
                                                                  })
    //TODO More advanced simplifications can be done, such as removing only elements that are very "surrounded" (i.e. they landed in various large buckets)
    /*val completeNodes=if (maxComparisonsPerItem.isDefined)
                        currentGraph.filter({case (index, (viewed,groupedList)) => (viewed>maxComparisonsPerItem.get) && (groupedList.forall({case (grId,list) => list.toSet.size>=1.0}) && (groupedList.size>=grouper.numGroups))})
                      else//Remove only elements that already have all their neighbors
                        currentGraph.filter({case (index, (viewed,groupedList)) => groupedList.forall({case (grId,list) => list.toSet.size>=numNeighbors})})
                        
    val deletableElements=completeNodes
    //Remove deletable elements from dataset
    return dataset.leftOuterJoin(deletableElements).flatMap({case (index, (neighbors1, n)) =>
                                                                if (n==None)
                                                                  Some((index, (neighbors1._1, neighbors1._2)))
                                                                else
                                                                  None
                                                                  })*/
  }
}

class LSHLookupKNNGraphBuilder(data:RDD[(Long,LabeledPoint)]) extends LSHKNNGraphBuilder
{
  var lookup:BroadcastLookupProvider=new BroadcastLookupProvider(data)
  
  override def splitLargeBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], maxBucketSize:Int, radius:Double, hasher:Hasher):RDD[(Hash, Iterable[Long], Int)]=
  {
    val l=lookup
    hashBuckets.flatMap({case (k, s, n) => s.map({ x => (k,x,n) })})
                   .flatMap({case(k, x, bucketSize) => if (bucketSize<maxBucketSize) Some((k,x))
                                                       else hasher.getHashes(l.lookup(x).features, x, radius).map({case (nk,i) => (k.concat(nk),i)})}) //Concat new key
                   .groupByKey()
                    .map({case (k, l) => (k, l.toSet)})
                    .flatMap({case (k, s) => if (s.size>1) Some((k, s, s.size)) else None})
  }
  
  override def getGroupedGraphFromBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.filter(_._2.size>1)
              //TODO Possibly repartition after filter
              //.repartition
             .flatMap({case (hash, indices, size) =>
                         //Remove duplicates from indices
                         val arrayIndices=indices.toSet.toArray
                         if (arrayIndices.length>1)
                           BruteForceKNNGraphBuilder.computeGroupedGraph(arrayIndices, l, numNeighbors, measurer, grouper)
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case ((viewed1,groupedNeighbors1), (viewed2,groupedNeighbors2)) => (viewed1+viewed2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                           })
             .partitionBy(data.partitioner.getOrElse(new HashPartitioner(data.getNumPartitions)))
    graph
  }
  
  override def getGroupedGraphFromPartitionedBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Iterable[Long])], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.flatMap({case (hash, l1, l2) =>
                                         BruteForceKNNGraphBuilder.computeGroupedGraph(l1, l2, l, numNeighbors, measurer, grouper)
                                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case ((viewed1,groupedNeighbors1), (viewed2,groupedNeighbors2)) => (viewed1+viewed2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                           })
             .partitionBy(data.partitioner.getOrElse(new HashPartitioner(data.getNumPartitions)))
    graph
  }
  
  override def getGroupedGraphFromElementIndexLists(data:RDD[(Long,LabeledPoint)], elementIndexLists:RDD[Iterable[Long]], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=elementIndexLists.filter(_.size>1)
              //TODO Possibly repartition after filter
              //.repartition
             .flatMap({case (indices) =>
                         //Remove duplicates from indices
                         val arrayIndices=indices.toSet.toArray
                         if (arrayIndices.length>1)
                           BruteForceKNNGraphBuilder.computeGroupedGraph(arrayIndices, l, numNeighbors, measurer)
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case ((viewed1,groupedNeighbors1), (viewed2,groupedNeighbors2)) =>
                             (viewed1+viewed2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                           })
                           
    graph
  }
  
  override def getGroupedGraphFromPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[((Long, LabeledPoint), Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.map(
                {
                  case ((i1,p1),i2) =>
                    val p2=l.lookup(i2)
                    ((i1,grouper.getGroupId(p2)), (1,List((i2, measurer.getDistance(p1, p2)))))
                })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case ((v1,neighbors1), (v2,neighbors2)) =>
                             (v1+v2,GraphMerger.mergeNeighborLists(neighbors1, neighbors2, numNeighbors))
                           })
             .map(
                 {
                   case ((i1,grId2),(v,neighborList)) => (i1,(v,List((grId2,neighborList))))
                 }
                 )
             .reduceByKey(
                 {
                   case ((v1,groupedNeighbors1),(v2,groupedNeighbors2)) =>
                     (v1+v2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                 }
                 )
                 
    graph
  }
  
  override def getGroupedGraphFromIndexPairs(data:RDD[(Long,LabeledPoint)], pairs:RDD[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):RDD[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]=
  {
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.flatMap({case (i1,i2) =>
                                var p1=l.lookup(i1)
                                var p2=l.lookup(i2)
                                val d=measurer.getDistance(p1, p2)
                                val counts1=BDV.zeros[Int](grouper.numGroups)
                                counts1(grouper.getGroupId(p2))+=1
                                val counts2=BDV.zeros[Int](grouper.numGroups)
                                counts2(grouper.getGroupId(p1))+=1
                                List[((Long,Int),(BDV[Int],List[(Long,Double)]))](((i1,grouper.getGroupId(p2)), (counts1,List((i2, d)))),((i2,grouper.getGroupId(p1)), (counts2,List((i1, d)))))
                            })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case ((v1,neighbors1), (v2,neighbors2)) =>
                             (v1+v2,GraphMerger.mergeNeighborLists(neighbors1, neighbors2, numNeighbors))
                           })
             .map(
                 {
                   case ((i1,grId2),(v,neighborList)) => (i1,(v,List((grId2,neighborList))))
                 }
                 )
             .reduceByKey(
                 {
                   case ((v1,groupedNeighbors1),(v2,groupedNeighbors2)) =>
                     (v1+v2,GraphMerger.mergeGroupedNeighbors(groupedNeighbors1, groupedNeighbors2, numNeighbors))
                 }
                 )
                           
    graph
  }
}

/*object LSHGraphXKNNGraphBuilder// extends LSHKNNGraphBuilder
{
  def getGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, dimension:Int)=
  {
    val radius=0.5
    val hasher=new EuclideanLSHasher(dimension)
    val hashRDD=data.flatMap({case (point, index) =>
                              hasher.getHashes(point.features, index, radius)
                            });
    val hashBuckets=hashRDD.groupByKey()
    val closeEdges=hashBuckets.filter(_._2.size>1)
                           //.repartition
                           .flatMap({case (hash, indices) =>
                                       //Remove duplicates from indices
                                       val arrayIndices=indices.toSet.toArray
                                       if (arrayIndices.length>1)
                                       {
                                         var list:List[Pair[Long, Long]]=List()
                                         //Cartesian product
                                         for (i <- 0 until arrayIndices.length)
                                           for (j <- i+1 until arrayIndices.length)
                                           {
                                             list=(arrayIndices(i), arrayIndices(j)) :: (arrayIndices(j), arrayIndices(i)) :: list
                                           }
                                         
                                         list
                                       }
                                       else
                                         Nil
                                       })
                                      
      val graph=GraphUtils.calculateNearest(data,
                                            numNeighbors,
                                            {case (x,y) => Vectors.sqdist(x.features, y.features)},
                                            closeEdges)
                                            
      graph
  }
  /*override def getGraphFromBuckets(data:RDD[(LabeledPoint,Long)], hashBuckets:RDD[(Hash, Iterable[Long])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    
  }*/
}*/