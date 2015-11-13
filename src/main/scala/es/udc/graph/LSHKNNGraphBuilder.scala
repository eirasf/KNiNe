package es.udc.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils
import org.apache.spark.mllib.linalg.Vectors
import es.udc.graph.utils.GraphUtils

// TODO The graph representation that is more suitable for the computations needs to be identified and used

abstract class LSHKNNGraphBuilder
{
  protected final def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, dimension:Int, hasher:Hasher):RDD[(Long, List[(Long, Double)])]=
  {
    //TODO This should be done iteratively for different radiuses
    var fullGraph:RDD[(Long, List[(Long, Double)])]=null
    var currentData=data.map(_.swap)
    var radius=0.1

    //DEBUG
    var i=0
    
    while(!currentData.isEmpty() && (i<5)/*DEBUG*/)
    {
      //Maps each element to numTables (hash, index) pairs with hashes of keyLenght length.
      val hashRDD=currentData.flatMap({case (index, point) =>
                                        hasher.getHashes(point.features, index, radius) //TODO Hash should take radius into account
                                      });
      
      /*
      //Print hashes
      hashRDD.foreach({case (hash, index) =>
                        var result=index+" - ("
                        var strHash=""
                        for (i <- 0 until keyLength)
                          strHash+=hash.values(i)+"#"
                        result+=strHash+", "
                        println(result+")")
                      })
       */
      
      //TODO Should all distances be computed? Maybe there's no point in computing them if we still don't have enough neighbors for an example
      //Should they be stored/cached? It may be enough to store a boolean that records if they have been computed. LRU Cache?
      //How will the graph be represented? Maybe an index RDD to be joined with the result of each step?
      
      //Groups elements mapped to the same hash
      val hashBuckets=hashRDD.groupByKey()
      
      /*
      //Print buckets
      println("Buckets:")
      hashBuckets.foreach({case (hash, indices) => println(hash.values + " -> " + indices)})
      */
      
      //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.
      
      val subgraph=getGraphFromBuckets(data, hashBuckets, numNeighbors)
      
      //TODO Check neighbors of destination vertices
      
      //Merge generate graph with existing one.
      if (fullGraph==null)
        fullGraph=subgraph
      else
      {
        /*fullGraph=fullGraph.fullOuterJoin(subgraph).map({case (index, (neighbors1, neighbors2)) =>
                                                      if (neighbors1==None)
                                                        (index, neighbors2.get)
                                                      else
                                                        if (neighbors2==None)
                                                          (index, neighbors1.get)
                                                        else
                                                         (index, mergeNeighborLists(neighbors1.get, neighbors2.get, numNeighbors))
                                                  })*/
        val f=mergeNeighborLists
        fullGraph=fullGraph.union(subgraph).reduceByKey({case (neighbors1, neighbors2) =>
                                                            f(neighbors1, neighbors2, numNeighbors)
                                                          })
      }
      //TODO Check for duplicates
      //Simplify dataset
      //Remove only elements that already have all their neighbors
      val completeNodes=fullGraph.filter({case (index, list) => list.size>=numNeighbors})
      println("Complete nodes:")
      completeNodes.foreach(println)
      //Still have to filter out those that link to an element that will be removed
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
      println("To be removed:")
      deletableElements.foreach(println)
      //TODO Remove only elements that are very "surrounded" (i.e. they landed in various large buckets)
      currentData=currentData.leftOuterJoin(deletableElements).flatMap({case (index, (neighbors1, n)) =>
                                                                  if (n==None)
                                                                    Some((index, neighbors1))
                                                                  else
                                                                    None
                                                                    })
      //TODO Possibly repartition
      currentData.cache()
      //Increment radius
      radius*=2
      //DEBUG
      println("Step "+i+ " - "+currentData.count()+" nodes left")
      fullGraph.foreach(println(_))
      i+=1
    }
    return fullGraph
  }
  
  def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int, dimension:Int):RDD[(Long, List[(Long, Double)])]=computeGraph(data,
                                                                                                                                  numNeighbors,
                                                                                                                                  dimension,
                                                                                                                                  new EuclideanLSHasher(dimension)) //Default to an EuclideanHasher
  
  def computeGraph(data:RDD[(LabeledPoint,Long)], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=computeGraph(data,
                                                                                                                   numNeighbors,
                                                                                                                   data.map({case (point, index) => point.features.size}).max()) //Get dimension from dataset
                                                                                                                            
  protected def getGraphFromBuckets(data:RDD[(LabeledPoint,Long)], hashBuckets:RDD[(Hash, Iterable[Long])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]
  
  val mergeNeighborLists = (neighbors1:List[(Long, Double)], neighbors2:List[(Long, Double)], numNeighbors:Int) =>
      {
        var sNeighbors1=neighbors1.sortBy(_._2)
         var sNeighbors2=neighbors2.sortBy(_._2)
         
         var finalNeighbors:List[(Long, Double)]=Nil
         
         while(finalNeighbors.size<numNeighbors && (!sNeighbors1.isEmpty || !sNeighbors2.isEmpty))
         {
           if (sNeighbors2.isEmpty || (!sNeighbors1.isEmpty && sNeighbors1.head._2<sNeighbors2.head._2))
           {
             finalNeighbors=sNeighbors1.head :: finalNeighbors
             sNeighbors1=sNeighbors1.tail
           }
           else
           {
             finalNeighbors=sNeighbors2.head :: finalNeighbors
             sNeighbors2=sNeighbors2.tail
           }
         }
         finalNeighbors
      }
}

object LSHLookupKNNGraphBuilder extends LSHKNNGraphBuilder
{
  override def getGraphFromBuckets(data:RDD[(LabeledPoint,Long)], hashBuckets:RDD[(Hash, Iterable[Long])], numNeighbors:Int):RDD[(Long, List[(Long, Double)])]=
  {
    val lookup=new BroadcastLookupProvider(data)
    
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.filter(_._2.size>1)
              //TODO Possibly repartition after filter
              //.repartition
             .flatMap({case (hash, indices) =>
                         //Remove duplicates from indices
                         val arrayIndices=indices.toSet.toArray
                         if (arrayIndices.length>1)
                           LocalBruteForceKNNGraphBuilder.computeGraph(arrayIndices, lookup, numNeighbors)
                         else
                           Nil
                         })
             //Merge neighbors found for the same element in different hash buckets
             .reduceByKey({case (neighbors1, neighbors2) =>
                             mergeNeighborLists(neighbors1, neighbors2, numNeighbors)
                           })
                           
    graph
  }
}

object LSHGraphXKNNGraphBuilder// extends LSHKNNGraphBuilder
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
}