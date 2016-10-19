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

object KNiNeTransform
{
    def main(args: Array[String])
    {
      if (args.length <= 0)
      {
        println("An input file must be provided")
        return
      }
      
      var file=args(0)
      
      //Set up Spark Context
      val sc=sparkContextSingleton.getInstance()
      
      //Stop annoying INFO messages
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.WARN)
      
      //Load data from file
      val rawData=sc.textFile(file)
      val data: RDD[(LabeledPoint, Long)] = rawData.map({ line => val values=line.split(";")
                                                                  (new LabeledPoint(0.0, Vectors.dense(values.slice(1, values.length).map { x => x.toDouble })), values(0).toLong)
                                                        })
      
      val n=data.count()
      println("Dataset has "+n+" elements")
      
      val numNeighbors=10 //TODO Should be input from user
                 
      /* LOOKUP VERSION */
      
      val graph=LSHLookupKNNGraphBuilder.computeGraph(data, numNeighbors)
      //Print graph
      /*println("There goes the graph:")
      graph.foreach({case (elementIndex, neighbors) =>
                      for(n <- neighbors)
                        println(elementIndex+"->"+n._1+"("+n._2+")")
                    })*/
      /**/
                    
      /* BRUTEFORCE VERSION  
      
      val graph=BruteForceKNNGraphBuilder.parallelComputeGraph(data, numNeighbors)
      //Print graph
      /*println("There goes the graph:")
      graph.foreach({case (elementIndex, neighbors) =>
                      for(n <- neighbors)
                        println(elementIndex+"->"+n._1+"("+n._2+")")
                    })*/
      */
      val edges=graph.flatMap({case (index, neighbors) => neighbors.map({case (destination, distance) => (index, destination)})})
      
      edges.foreach(println(_))
      //edges.saveAsTextFile(file+"_transformed-kNiNe")
      
      //Stop the Spark Context
      sc.stop()
    }
  }