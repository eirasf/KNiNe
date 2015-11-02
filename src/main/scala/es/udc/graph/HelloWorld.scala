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

object HelloWorld
{
  private val OptimalW=4
  
    def main(args: Array[String])
    {
      if (args.length <= 0)
      {
        println("An input libsvm file must be provided")
        return
      }
      
      println("STARTING")
      
      var file=args(0)
      
      //Set up Spark Context
      val conf = new SparkConf().setAppName("TestStrath").setMaster("local")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
//      conf.set("spark.eventLog.enabled", "true")
//      conf.set("spark.eventLog.dir","file:///home/eirasf/Escritorio/Tmp-work/sparklog-local")
      val sc=new SparkContext(conf)
      
      //Load data from file
      val data: RDD[(LabeledPoint, Long)] = MLUtils.loadLibSVMFile(sc, file).zipWithIndex()
      
      //TODO Create index RDD if necessary
      //TODO Normalize if necessary

      val n=data.count()
      println("Dataset has "+n+" elements")
      
      val numTables=4
      val keyLength=5
      val dimension=5
      val w=OptimalW
      
      //TODO Should be performed in Hasher.init(numTables, keyLength)
      val gaussianVectors=ofDim[Double](numTables, keyLength, dimension)
      val b=ofDim[Double](numTables, keyLength)
      val randomGenerator=new Random()
      for(i <- 0 until numTables)
        for (j <- 0 until keyLength)
        {
          for (k <- 0 until dimension)
            gaussianVectors(i)(j)(k)=randomGenerator.nextGaussian()
          b(i)(j)=randomGenerator.nextDouble*w
        }
      //Generate b
          
      val hashRDD=data.map({case (point, index) =>
                              //TODO This whole function should be in Hasher.computeHashes(Vector) 
                              val hashes=ofDim[Integer](numTables, keyLength)
                              for(i <- 0 until numTables)
                                for (j <- 0 until keyLength)
                                {
                                  var dotProd:Double=0
                                  //TODO Take dot product to a function or used a prebuilt one
                                  if (point.features.isInstanceOf[DenseVector])
                                  {
                                    for (k <- 0 until dimension)
                                      dotProd+=point.features(k) * gaussianVectors(i)(j)(k)
                                  }
                                  else //SparseVector
                                  {
                                    val sparse=point.features.asInstanceOf[SparseVector]
                                    val indices=sparse.indices
                                    val values=sparse.values
                                    
                                    for (k <- 0 until indices.length)
                                    {
                                      //if (indices(k)>=dimension)
                                      //  break
                                      if (indices(k)<dimension)
                                        dotProd+=values(k) * gaussianVectors(i)(j)(indices(k))
                                    }
                                  }
                                  hashes(i)(j)=math.floor((dotProd + b(i)(j))/w).toInt
                                }
                              (index, hashes)});
      
      hashRDD.foreach({case (index, hashes) =>
                        var result=index+" - ("
                        for (i <- 0 until numTables)
                        {
                          var strHash=""
                          for (j <- 0 until keyLength)
                          {
                            strHash+=hashes(i)(j)+"#"
                          }
                          result+=strHash+", "
                        }
                        println(result+")")
                      })
      
      //Stop the Spark Context
      sc.stop()
    }
  }