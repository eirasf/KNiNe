package es.udc.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils
import org.apache.spark.HashPartitioner
import breeze.linalg.{DenseVector => BDV}



abstract class SimpleLSHKNNGraphBuilder extends GraphBuilder
{
  final def computeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, hasher:Hasher, measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]=
  {
    val totalElements=data.count()
    val bfOps:Double=totalElements*(totalElements-1)/2.0
    var totalOps:Long=0
    val bHasher=data.sparkContext.broadcast(hasher)
    val radius=1.0
    
    println(f"Starting simple $numNeighbors%d-NN graph computation for $totalElements%d nodes")
  
    //Maps each element to numTables (hash, index) pairs with hashes of keyLength length.
    /*val hashRDD=data.flatMap({case (index, point) => bHasher.value.getHashes(point.features, index, radius)})
                    .coalesce(data.getNumPartitions)*/
    val hashRDD=hasher.hashData(data, radius)
                      .coalesce(data.getNumPartitions)
    
    
    //Groups elements mapped to the same hash
    var hashBuckets:RDD[(Hash, Iterable[Long], Int)]=hashRDD.groupByKey()
                                                            .map({case (k, l) =>
                                                              (k, l.toSet)})
                                                            .flatMap({case (k, s) => if (s.size>1) Some((k, s, s.size)) else None})
    hashBuckets=hashBuckets.coalesce(data.getNumPartitions).cache()
    if (hashBuckets.isEmpty())
    {
      println("Hashing procedure yielded no buckets with more than one element")
      return null
    }
    
    val numBuckets=hashBuckets.count()
    val stepOps=hashBuckets.map({case (h,s,n) => (n,1)})
                   .reduceByKey(_+_)

    val numStepOps=stepOps.map({case x => x._2 * x._1 * (x._1 - 1) /2.0}).sum().toLong
    val largestBucketSize=stepOps.map(_._1).max
    //stepOps.sortByKey(true, 64).foreach(println) //DEBUG Check bucket sizes
    totalOps=totalOps+numStepOps

    println(f"Performing $numStepOps%g ops (largest bucket has $largestBucketSize%d elements) - Scan rate=${100*totalOps/bfOps}%.4f%%")
    return getGraphFromBuckets(data, hashBuckets, numNeighbors, measurer).coalesce(data.getNumPartitions)
  }
  
  def computeGraph(data:RDD[(Long,LabeledPoint)], numNeighbors:Int, hasherKeyLength:Int, hasherNumTables:Int, measurer:DistanceProvider, blockSz:Option[Int]):RDD[(Long, List[(Long, Double)])]
            =computeGraph(data,
                           numNeighbors,
                           if (blockSz.isDefined) new EuclideanProjectedLSHasher(data.map({case (index, point) => point.features.size}).max(), hasherKeyLength, hasherNumTables, blockSz.get)
                           else new EuclideanLSHasher(data.map({case (index, point) => point.features.size}).max(), hasherKeyLength, hasherNumTables),//Get dimension from dataset
                           measurer)
                                                                                           
  protected def getGraphFromBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]
}

class SimpleLSHLookupKNNGraphBuilder(data:RDD[(Long,LabeledPoint)]) extends SimpleLSHKNNGraphBuilder
{
  var lookup:BroadcastLookupProvider=new BroadcastLookupProvider(data)
  
  override def getGraphFromBuckets(data:RDD[(Long,LabeledPoint)], hashBuckets:RDD[(Hash, Iterable[Long], Int)], numNeighbors:Int, measurer:DistanceProvider):RDD[(Long, List[(Long, Double)])]=
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
                         {
                           //Use a dummy grouping provider since this contemplates no groups
                           val g=BruteForceKNNGraphBuilder.computeGroupedGraph(arrayIndices, l, numNeighbors, measurer, new DummyGroupingProvider())
                           g.map({case (id,(viewed,groupedNeighbors)) => (id, groupedNeighbors.head._2)})
                         }
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>val newList=(neighbors1++neighbors2).toSet.toList
                                                            if (newList.size<=numNeighbors)
                                                              newList
                                                            else
                                                              newList.sortBy(_._2).take(numNeighbors)
                           })
             .partitionBy(data.partitioner.getOrElse(new HashPartitioner(data.getNumPartitions)))
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