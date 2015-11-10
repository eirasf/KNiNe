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

object sparkContextSingleton
{
  @transient private var instance: SparkContext = _
  private val conf : SparkConf = new SparkConf().setAppName("TestStrath")
                                                .setMaster("local")
                                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                                                //.set("spark.eventLog.enabled", "true")
                                                //.set("spark.eventLog.dir","file:///home/eirasf/Escritorio/Tmp-work/sparklog-local")

  def getInstance(): SparkContext=
  {
    if (instance == null)
      instance = new SparkContext(conf)
    instance
  }  
}

object Strath
{
    def main(args: Array[String])
    {
      if (args.length <= 0)
      {
        println("An input libsvm file must be provided")
        return
      }
      
      var file=args(0)
      
      //Set up Spark Context
      val sc=sparkContextSingleton.getInstance()
      
      //Load data from file
      val data: RDD[(LabeledPoint, Long)] = MLUtils.loadLibSVMFile(sc, file).zipWithIndex()
      
      //TODO Normalize if necessary

      val n=data.count()
      val dimension=data.map(_._1.features.size).max() //TODO Dimension should be either read from the dataset or input by the user
      println("Dataset has "+n+" elements and dimension +"+dimension)
      
      //TODO This should be done iteratively for different radiuses
      val numNeighbors=2 //TODO Should be input from user
      
      /*GRAPH VERSION
      
      val graph=LSHGraphXKNNGraphBuilder.getGraph(data, numNeighbors, dimension)
      println("There goes the graph:")
      graph.foreach(println(_))
      
      */
      
      
      /* LOOKUP VERSION
      
      val graph=LSHLookupKNNGraphBuilder.computeGraph(data, numNeighbors, dimension)
      //Print graph
      println("There goes the graph:")
      graph.foreach({case (elementIndex, neighbors) =>
                      for(n <- neighbors)
                        println(elementIndex+"->"+n._1+"("+n._2+")")
                    })
      */
                    
      /* BRUTEFORCE VERSION */
      
      val graph=LocalBruteForceKNNGraphBuilder.computeGraph(data, numNeighbors)
      //Print graph
      println("There goes the graph:")
      graph.foreach({case (elementIndex, neighbors) =>
                      for(n <- neighbors)
                        println(elementIndex+"->"+n._1+"("+n._2+")")
                    })
      /**/
      
      //Stop the Spark Context
      sc.stop()
    }
  }