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

object CompareGraphs
{
    def compare(fileExact:String, file:String):RDD[(Long,(Double, Set[(Long,Double)],Double, Set[(Long,Double)]))]=
    {
      val sc=sparkContextSingleton.getInstance()
      //Load data from files
      val rawDataExact=sc.textFile(fileExact)
      val dataExact: RDD[(Long, (Long,Double))] = rawDataExact.map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                                            (values(0).toLong, (values(1).toLong, values(2).toDouble))
                                                          })
                                                          
      val rawData=sc.textFile(file)
      val data: RDD[(Long, Iterable[(Long,Double)])] = rawData.map({ line => val values=line.substring(1, line.length()-1).split(",")
                                                          (values(0).toLong, (values(1).toLong, values(2).toDouble))
                                                }).groupByKey()
      
      val totalEdges=dataExact.count()
      
//println("The graph has "+totalEdges+" edges")
      
      val commonEdges=dataExact.groupByKey().join(data)
                                            .map({case (element, (neighborsExact, neighbors)) => val intersect=neighbors.map(_._1).toSet.intersect(neighborsExact.map(_._1).toSet)
                                                                                                 var result=intersect.size
                                                                                                 if (result<neighbors.size)
                                                                                                     result=neighbors.map(_._2).toSet.intersect(neighborsExact.map(_._2).toSet).size
                                                                                                 result})
                                                                                                 
                                            
                                            .sum()
                                            
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
     /*r.filter(_._1<100)
                            .sortBy(_._1)
                            .foreach(println(_))*/
                               
//println("The aprox. graph has "+commonEdges+" edges in common ("+(commonEdges.toDouble/totalEdges.toDouble)+")")
println(commonEdges.toDouble/totalEdges.toDouble)
      return r
    }
    def main(args: Array[String])
    {
      if (args.length <= 1)
      {
        println("Two input files must be provided")
        return
      }
      
      val fileExact=args(0)
      val file=args(1)
      
      //Set up Spark Context
      val sc=sparkContextSingleton.getInstance()
      
      //Stop annoying INFO messages
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.WARN)
      
      compare(fileExact, file)
      
      //Stop the Spark Context
      sc.stop()
    }
  }