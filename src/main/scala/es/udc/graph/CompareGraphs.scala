package es.udc.graph

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import es.udc.graph.utils.GraphUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner

object CompareGraphs
{
    def compare(fileExact:String, file:String, dataset:Option[String]):(Double,Double,Option[Double],Option[Double])=
    {
      val sc=sparkContextSingleton.getInstance()
      //Load data from files
      val rawDataExact=sc.textFile(fileExact)
      val dataExact: RDD[(Long, (Long,Double))] = rawDataExact.map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                                            (values(0).toLong, (values(1).toLong, values(2).toDouble))
                                                          })
                                                          //.filter(_._1<10) //DEBUG - Short
      var rawData=sc.textFile(file).map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                          (values(0).toLong, values(1).toLong, values(2).toDouble)
                                                })
      var hasDistances=rawData.map(_._3).distinct().count()>1
      if (!hasDistances && dataset.isDefined)
      {
        hasDistances=true
        //Load data from file
        val datasetRDD: RDD[(Long,LabeledPoint)] = MLUtils.loadLibSVMFile(sc, dataset.get).zipWithIndex().map(_.swap).partitionBy(new HashPartitioner(KNiNe.DEFAULT_NUM_PARTITIONS.toInt))
        val lookup:BroadcastLookupProvider=new BroadcastLookupProvider(datasetRDD)
        val measurer=new EuclideanDistanceProvider()
        rawData=rawData.flatMap({case (index1, index2, nothing) =>
                              if ((index1>=0) && (index2>=0))
                                Some((index1,index2,measurer.getDistance(lookup.lookup(index1), lookup.lookup(index2))))
                              else
                                None
                            })
      }
      
      val data: RDD[(Long, Iterable[(Long,Double)])] = rawData.map({case (e1,e2,d) => (e1,(e2,d))}).partitionBy(new HashPartitioner(KNiNe.DEFAULT_NUM_PARTITIONS.toInt)).groupByKey()
      
      /*
      val n=24864
      data.filter(_._1==n).foreach(_._2.foreach(println))
      
      println("-------------------")
      
      dataExact.filter(_._1==n).foreach(println)
      System.exit(0)
      */
      val totalEdges=dataExact.count()
      
//println("The graph has "+totalEdges+" edges")
      
      val avgNumNeighbors=data.map({case (element, neighbors) => neighbors.size}).mean()
      
      val recall=dataExact.groupByKey().join(data)
                                            .map({case (element, (neighborsExact, neighbors)) => val intersect=neighbors.map(_._1).toSet.intersect(neighborsExact.map(_._1).toSet)
                                                                                                 var result=intersect.size
                                                                                                 //if (result<neighbors.size)
                                                                                                 //    result=neighbors.map(_._2).toSet.intersect(neighborsExact.map(_._2).toSet).size
                                                                                                 result.toFloat/neighborsExact.size})
                                                                                                 
                                            
                                            .mean()
      
      val recallDistanceBased:Option[Double]=if (!hasDistances) None
          else
          {
            val r=dataExact.groupByKey().join(data)
                                            .map({case (element, (neighborsExact, neighbors)) =>
                                                    val maxD=neighborsExact.map(_._2).max
                                                    neighbors.filter(_._2<=maxD).size.toFloat/neighborsExact.size
                                                    //neighbors.filter(_._2-maxD<0.00001).size.toFloat/neighborsExact.size
                                            })
                                            .mean()
            Some(r)
          }
      val distanceError:Option[Double]=if (!hasDistances) None
          else
          {
            val r=dataExact.groupByKey().join(data)
                                            .map({case (element, (neighborsExact, neighbors)) =>
                                                    val sumDExact=neighborsExact.map(_._2).sum
                                                    val sumD=neighborsExact.size*neighbors.map(_._2).sum/neighbors.size
                                                    (sumD-sumDExact)/neighborsExact.size
                                            })
                                            .mean()
            Some(r)
          }
      return (avgNumNeighbors,recall,recallDistanceBased,distanceError)
      /*                                      
      var r=dataExact.groupByKey().join(data)
                            .flatMap({case (element, (neighborsExact, neighbors)) => val intersect=neighbors.map(_._1).toSet.intersect(neighborsExact.map(_._1).toSet)
                                                                                 var result=intersect.size
                                                                                 if (result<neighbors.size)
                                                                                 {
                                                                                     val exactSet=neighborsExact.toSet
                                                                                     val compareSet=neighbors.toSet
                                                                                     Some((element, (exactSet.map(_._2).max, exactSet.diff(compareSet), compareSet.map(_._2).max, compareSet.diff(exactSet))))
                                                                                 }
                                                                                 else
                                                                                     None})
                               
//println("The aprox. graph has "+commonEdges+" edges in common ("+(commonEdges.toDouble/totalEdges.toDouble)+")")
println(commonEdges.toDouble/data.first()._2.size)
      return r*/
    }
    
    def comparePositions(fileExact:String, file:String)=
    {
      val sc=sparkContextSingleton.getInstance()
      //Load data from files
      val rawDataExact=sc.textFile(fileExact)
      val dataExact: RDD[(Long, (Long,Double))] = rawDataExact.map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                                            (values(0).toLong, (values(1).toLong, values(2).toDouble))
                                                          })
                                                          //DEBUG - .filter(_._1<10) //Short
      val rawData=sc.textFile(file)
      val data: RDD[(Long, Iterable[(Long,Double)])] = rawData.map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                          (values(0).toLong, (values(1).toLong, values(2).toDouble))
                                                })
                                                //DEBUG - .filter(_._1<10) //Short
                                                .groupByKey()
      
//println("The graph has "+totalEdges+" edges")
      
      val commonEdges=dataExact.groupByKey().join(data)
                                            .map({case (element, (neighborsExact, neighbors)) => var nE=neighborsExact.toArray
                                                                                                 var n=neighbors.toSet.toArray
                                                                                                 var total:Double=0
                                                                                                 val k=neighbors.size
                                                                                                 val maxN:Int=Math.min(2*k, neighborsExact.size)
                                                                                                 for (i <- 0 until n.size)
                                                                                                 {
                                                                                                   var j=0
                                                                                                   var found=false
                                                                                                   while ((j < maxN) && !found)
                                                                                                   {
                                                                                                     if (nE(j)._1==n(i)._1)
                                                                                                     {
                                                                                                       if (j<k)
                                                                                                         total=total+1
                                                                                                       else
                                                                                                         total=total+1-((j-k+1)/k)
                                                                                                       found=true
                                                                                                     }
                                                                                                     j=j+1
                                                                                                   }
                                                                                                 }
                                                                                                 total/k})
                                            .mean()
                                            
     /*r.filter(_._1<100)
                            .sortBy(_._1)
                            .foreach(println(_))*/
                               
      println("The aprox. graph has "+commonEdges+" edges in common")
      //println(commonEdges.toDouble)
    }
    
    def printResults(results:(Double,Double,Option[Double],Option[Double]))=
    {
      val (avgNumNeighbors,recall,recallDistanceBased,distanceError)=results
      println("Average number of neighbors: "+avgNumNeighbors)
      println("Recall: "+recall)
      println("Recall (distance-based): "+(if (recallDistanceBased.isDefined) recallDistanceBased.get else "-"))
      println("Distance error: "+(if (distanceError.isDefined) distanceError.get else "-"))
    }
    
    def main(args: Array[String])
    {
      if (args.length <= 1)
      {
        println("""Usage: CompareGraphs approxGraph exactGraph [dataset]
        Graphs must be text files with a (elem1,elem2,dist) per tuple
        If dist is 0 for all tuples and dataset (in libsvm format) is provided, distances will be recalculated.""")
        return
      }
      
      val file=args(0)
      val fileExact=args(1)
      val dataset:Option[String]=if (args.length<3) None else Some(args(2))
      
      //Set up Spark Context
      val sc=sparkContextSingleton.getInstance()
      
      //Stop annoying INFO messages
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.WARN)
      
      printResults(compare(fileExact, file, dataset))
      
      //Stop the Spark Context
      sc.stop()
    }
  }