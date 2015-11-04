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

class Hash(val values: Array[Integer]) extends Serializable {
    override val hashCode = values.deep.hashCode
    override def equals(obj:Any) = obj.isInstanceOf[Hash] && obj.asInstanceOf[Hash].values.deep == this.values.deep
}

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
      
      //TODO Normalize if necessary

      val n=data.count()
      println("Dataset has "+n+" elements")
      
      //TODO This should be done iteratively for different radiuses
      
      val numTables=4
      val keyLength=5
      val dimension=5 //TODO Dimension should be either read from the dataset or input by the user
      val w=OptimalW
      val numNeighbors=2
      
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
          
      val hashRDD=data.flatMap({case (point, index) =>
                              //TODO This whole function should be in Hasher.computeHashes(Vector) 
                              var hashes=List[(Hash, Long)]()
                              for(i <- 0 until numTables)
                              {
                                val hash=new Array[Integer](keyLength)
                                for (j <- 0 until keyLength)
                                {
                                  var dotProd:Double=0
                                  //TODO Take dot product to a function or use a prebuilt one
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
                                  hash(j)=math.floor((dotProd + b(i)(j))/w).toInt
                                }
                                hashes=(new Hash(hash),index) :: hashes
                              }
                              hashes});
      
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
      //Should they be stored/cached?
      //How will the graph be represented? Maybe an index RDD to be joined with the result of each step? 
                      
      val byKey=hashRDD.groupByKey()
      byKey.foreach({case (hash, indices) => println(hash.values + " -> " + indices)})
      
      //Discard single element hashes and for the rest get every possible pairing to build graph
      //TODO Posibly repartition after filter
      byKey.filter(_._2.size>1)
           //.repartition
           .map({case (hash, indices) =>
                       //TODO Take all this to a function bruteForce(indices)
                       val arrayIndices=indices.toArray
                       val graphBuilder=new BruteForceKNNGraphBuilder(numNeighbors)
                       graphBuilder.computeGraph(arrayIndices)
                       })
      
      //Stop the Spark Context
      sc.stop()
    }
  }