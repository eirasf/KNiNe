package es.udc.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils

// TODO The graph representation that is more suitable for the computations needs to be identified and used
object LSHKNNGraphBuilder
{
  val DEFAULT_RADIUS_START=0.1
  
}

abstract class LSHKNNGraphBuilder
{
  protected final def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, dimension:Int, hasher:Hasher):RDD[(Long, List[(Long, Double)])]=
  {
    var fullGraph:RDD[(Long, List[(Long, Double)])]=null
    var currentData=data.map(_.swap)
    var radius=0.2//5//0.1
val totalElements=currentData.count()
val bfOps:Double=totalElements*totalElements///2.0
var totalOps:Long=0
var numBuckets:Long=2
    var nodesLeft=currentData.count()
    
    //while(!currentData.isEmpty())
    //while(nodesLeft>numNeighbors)
    while(numBuckets>1 && nodesLeft>1)
    {
      //Maps each element to numTables (hash, index) pairs with hashes of keyLenght length.
      val hashRDD=currentData.flatMap({case (index, point) =>
                                        hasher.getHashes(point.features, index, radius)
                                      });
      
      //TODO Should all distances be computed? Maybe there's no point in computing them if we still don't have enough neighbors for an example
      //Should they be stored/cached? It may be enough to store a boolean that records if they have been computed. LRU Cache?
      
      //Groups elements mapped to the same hash
      //val hashBuckets=hashRDD.groupByKey()
      val hashBuckets:RDD[(Hash, Iterable[Long])]=hashRDD.groupByKey().map({case (k, l) => (k, l.toSet)})
numBuckets=hashBuckets.count()
println("Buckets: "+numBuckets)
      /*
      //Print buckets
      println("Buckets:")
      hashBuckets.foreach({case (hash, indices) => println(hash.values.mkString(",") + " -> " + indices)})
      */
      val filteredBuckets=hashBuckets.map(_._2.size).filter(_>1)
      //println(filteredBuckets.count()+ " buckets ["+filteredBuckets.min()+", "+filteredBuckets.max()+"] elements")
      val stepOps=filteredBuckets.map((_,1))
                     .reduceByKey(_+_)

totalOps=totalOps+stepOps.map({case x => x._2 * x._1 * x._1 /2.0}).sum().toLong

      stepOps.sortBy(_._1)
                     .foreach({case x => println(x._2+" buckets with "+x._1+" elements => "+(x._2 * x._1 * x._1/2.0)+" ops")})

      
      
      //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.
      
      var subgraph=getGraphFromBuckets(data, hashBuckets, numNeighbors)
      
      //subgraph.foreach(println(_))
      //subgraph.sortBy(_._1).foreach({case (e,l) => println((e,l.sortBy(_._1)))})
      
      //Refine graph by checking neighbors of destination vertices
//TODO TEST
subgraph=refineGraph(data,subgraph,numNeighbors)
      
      //Merge generate graph with existing one.
      fullGraph=mergeSubgraphs(fullGraph, subgraph, numNeighbors)
      
      //TODO TEST
//fullGraph=refineGraph(data,fullGraph,numNeighbors)
      
      //TODO Check for duplicates
      //Simplify dataset
      currentData=simplifyDataset(currentData, fullGraph, numNeighbors)
      
      //TODO Possibly repartition
      currentData.cache()
      nodesLeft=currentData.count()
      //Increment radius
      //radius*=2
      radius*=2.0//1.4
      //DEBUG
      println(nodesLeft+" nodes left. Radius:"+radius)
      //fullGraph.foreach(println(_))
    }
    if (nodesLeft>0)
    {
      println("Elements left:"+currentData.map(_._1).collect().mkString(", "))
      if (fullGraph!=null)
      {
        //If there are any items left, look in the neighbor's neighbors.
        val neighbors:RDD[Iterable[Long]]=fullGraph.cartesian(currentData)
                                                   .flatMap({case ((dest, neig), (orig,point)) => if (neig.map(_._1).contains(orig))
                                                                                                    Some((orig,(dest :: neig.map(_._1)).toSet))
                                                                                                  else
                                                                                                    None })
                                                   .reduceByKey({case (dl1,dl2) => dl1 ++ dl2})
                                                   .map({case (o,dl) => dl + o})
totalOps=totalOps+neighbors.map({case x => x.size * x.size }).sum().toLong
        val subgraph=getGraphFromElementIndexLists(data, neighbors, numNeighbors)
        
        if (!subgraph.isEmpty())
        {
          subgraph.foreach(println(_))
          fullGraph=mergeSubgraphs(fullGraph, subgraph, numNeighbors)
          currentData=simplifyDataset(currentData, fullGraph, numNeighbors)
          nodesLeft=currentData.count() 
        }
        println(nodesLeft+" nodes left after first attempt")
      }
      if (nodesLeft>0) //No solution other than check this points with every other
      {
        val pairs=currentData.cartesian(fullGraph.map({case (point, neighbors) => point}))
        val subgraph=getGraphFromPairs(data, pairs, numNeighbors)
        fullGraph=mergeSubgraphs(fullGraph, subgraph, numNeighbors)
totalOps=totalOps+pairs.count()
      }
    }
println("Operations wrt bruteforce: "+(totalOps/bfOps))
    
    return fullGraph
  }
  
  def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, dimension:Int, hasherKeyLength:Int, hasherNumTables:Int):RDD[(Long, List[(Long, Double)])]=computeGraph(data,
                                                                                                                                  numNeighbors,
                                                                                                                                  dimension,
                                                                                                                                  new EuclideanLSHasher(dimension, hasherKeyLength, hasherNumTables)) //Default to an EuclideanHasher
  
  def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, hasherKeyLength:Int, hasherNumTables:Int):RDD[(Long, List[(Long, Double)])]
            =computeGraph(data,
                           numNeighbors,
                           data.map({case (point, index) => point.features.size}).max(), //Get dimension from dataset
                           hasherKeyLength:Int,
                           hasherNumTables:Int)
                                                                                                                            
  protected def getGraphFromBuckets(data:RDD[(LabeledPoint,Long)], hashBuckets:RDD[(Hash, Iterable[Long])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]
  
  protected def getGraphFromElementIndexLists(data:RDD[(LabeledPoint,Long)], elementIndexLists:RDD[Iterable[Long]], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]
  
  protected def getGraphFromPairs(data:RDD[(LabeledPoint,Long)], pairs:RDD[((Long, LabeledPoint), Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]
  
  protected def getGraphFromIndexPairs(data:RDD[(LabeledPoint,Long)], pairs:RDD[(Long, Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]
  
  private def simplifyDataset(dataset:RDD[(Long, LabeledPoint)], currentGraph:RDD[(Long, List[(Long, Double)])], numNeighbors:Int):RDD[(Long, LabeledPoint)]=
  {
    //TODO More advanced simplifications can be done, such as removing only elements that are very "surrounded" (i.e. they landed in various large buckets)
    //Remove only elements that already have all their neighbors
    //fullGraph.foreach(println(_))
    val completeNodes=currentGraph.filter({case (index, list) => list.toSet.size>=numNeighbors})
    //println("Complete nodes:")
    //completeNodes.foreach(println)
    //Still have to filter out those that link to an element that will be removed
    val deletableElements=completeNodes
    /*???????
    val deletableElements=completeNodes.flatMap({case (index, list) =>
                                                   list.map({case (dest, dist) => (dest, index)})
                                                })
                                      //Join with the full graph, to use edges that are in there (TODO CHECK)
                                      .join(fullGraph)
                                      .flatMap({case (index, (from, list)) => 
                                                  list.map({case (v, dist) => (index, v)})
                                               })
                                      .leftOuterJoin(completeNodes)
                                      .flatMap({case (dest, (origin, list)) =>
                                                  if (list==None)
                                                    Some((origin, 1))
                                                  else
                                                    None
                                                })
                                      .reduceByKey({case (n1, n2) => 1})
                                      
    */
    //println("To be removed:")
    //if (deletableElements.count()>1)
    //  deletableElements.foreach(println)
    //Remove deletable elements from dataset
    return dataset.leftOuterJoin(deletableElements).flatMap({case (index, (neighbors1, n)) =>
                                                                if (n==None)
                                                                  Some((index, neighbors1))
                                                                else
                                                                  None
                                                                  })
  }
  
  private def mergeSubgraphs(g1:RDD[(Long, List[(Long, Double)])], g2:RDD[(Long, List[(Long, Double)])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    if (g1==null) return g2
    if (g2==null) return g1
    val f=mergeNeighborLists
    return g1.union(g2).reduceByKey({case (neighbors1, neighbors2) =>
                                                      f(neighbors1, neighbors2, numNeighbors)
                                                    })
  }
  
  /**
 * @param g
 * @return Resulting graph of searching for closer neighbors in the neighbors of the neighbors for each node
 */
  /*private */def refineGraph(data:RDD[(LabeledPoint, Long)], g:RDD[(Long, List[(Long, Double)])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    var subgraph=getGraphFromIndexPairs(data,
                                        g.flatMap({case (node, neighbors) => neighbors.map({case (dest, dist) => (dest, node)})})
                                         .join(g)
                                         .flatMap({case (dest, (node, neighsNeighs)) => neighsNeighs.flatMap({case (d, dist) => if (node!=d)
                                                                                                                                  Some((node, d))
                                                                                                                                else
                                                                                                                                  None})}),
                                        numNeighbors)
    return mergeSubgraphs(g, subgraph, numNeighbors)
  }
  
  val mergeNeighborLists = (neighbors1:List[(Long, Double)], neighbors2:List[(Long, Double)], numNeighbors:Int) =>
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
}

object LSHLookupKNNGraphBuilder extends LSHKNNGraphBuilder
{
  var lookup:BroadcastLookupProvider=null
  override def getGraphFromBuckets(data:RDD[(LabeledPoint,Long)], hashBuckets:RDD[(Hash, Iterable[Long])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    if (lookup!=null)
      lookup.bData.destroy()
    lookup=new BroadcastLookupProvider(data)
    
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.filter(_._2.size>1)
              //TODO Possibly repartition after filter
              //.repartition
             .flatMap({case (hash, indices) =>
                         //Remove duplicates from indices
                         val arrayIndices=indices.toSet.toArray
                         if (arrayIndices.length>1)
                           BruteForceKNNGraphBuilder.computeGraph(arrayIndices, lookup, numNeighbors)
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>
                             mergeNeighborLists(neighbors1, neighbors2, numNeighbors)
                           })
    graph
  }
  
  override def getGraphFromElementIndexLists(data:RDD[(LabeledPoint,Long)], elementIndexLists:RDD[Iterable[Long]], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    if (lookup!=null)
      lookup.bData.destroy()
    lookup=new BroadcastLookupProvider(data)
    
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=elementIndexLists.filter(_.size>1)
              //TODO Possibly repartition after filter
              //.repartition
             .flatMap({case (indices) =>
                         //Remove duplicates from indices
                         val arrayIndices=indices.toSet.toArray
                         if (arrayIndices.length>1)
                           BruteForceKNNGraphBuilder.computeGraph(arrayIndices, lookup, numNeighbors)
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>
                             mergeNeighborLists(neighbors1, neighbors2, numNeighbors)
                           })
                           
    graph
  }
  
  override def getGraphFromPairs(data:RDD[(LabeledPoint,Long)], pairs:RDD[((Long, LabeledPoint), Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    val lookup=new BroadcastLookupProvider(data)
    
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.map({case ((i1,p1),i2) => (i1, List((i2, BruteForceKNNGraphBuilder.getDistance(p1, lookup.lookup(i2)))))})
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>
                             mergeNeighborLists(neighbors1, neighbors2, numNeighbors)
                           })
                           
    graph
  }
  
  override def getGraphFromIndexPairs(data:RDD[(LabeledPoint,Long)], pairs:RDD[(Long, Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    val lookup=new BroadcastLookupProvider(data)
    
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.map({case (i1,i2) => (i1, List((i2, BruteForceKNNGraphBuilder.getDistance(lookup.lookup(i1), lookup.lookup(i2)))))})
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>
                             mergeNeighborLists(neighbors1, neighbors2, numNeighbors)
                           })
                           
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