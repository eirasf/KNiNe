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

object HelloWorld
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
      val conf = new SparkConf().setAppName("TestStrath").setMaster("local")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
//      conf.set("spark.eventLog.enabled", "true")
//      conf.set("spark.eventLog.dir","file:///home/eirasf/Escritorio/Tmp-work/sparklog-local")
      val sc=new SparkContext(conf)
      
      //Load data from file
      val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, file)

      val n=data.count()
      println("Dataset has "+n+" elements")
      
      val numTables=4;
      val keyLenght=5;
      
      val gaussianVectors=ofDim[Double](10);
      
      //Stop the Spark Context
      sc.stop()
    }
  }