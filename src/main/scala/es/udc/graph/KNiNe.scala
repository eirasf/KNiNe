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

object sparkContextSingleton
{
  @transient private var instance: SparkContext = _
  private val conf : SparkConf = new SparkConf().setAppName("TestStrath")
                                                .setMaster("local")
                                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                                                .set("spark.eventLog.enabled", "true")
                                                .set("spark.eventLog.dir","file:///home/eirasf/Escritorio/sparklog-local")
                                                .set("spark.kryoserializer.buffer.max", "512")
                                                .set("spark.driver.maxResultSize", "2048")

  def getInstance(): SparkContext=
  {
    if (instance == null)
      instance = new SparkContext(conf)
    instance
  }  
}

object KNiNe
{
    def main(args: Array[String])
    {
      if (args.length <= 0)
      {
        println("An input libsvm file must be provided")
        return
      }
      
      //OPTIONS - TODO Should be input from user
      //val file=args(0)
      val file="/home/eirasf/Escritorio/kNNTEMP/car-dopado.libsvm"
      val numNeighbors=10
      //val METHOD="brute"
      val METHOD="lsh"
      val FORMAT=if ((file.length()>7) && (file.substring(file.length()-7) ==".libsvm"))
                   "libsvm"
                 else
                   "text"
      
      //Set up Spark Context
      val sc=sparkContextSingleton.getInstance()
      
      //Stop annoying INFO messages
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.WARN)
      
      //Load data from file
      val data: RDD[(LabeledPoint, Long)] = if (FORMAT=="libsvm")
                                              MLUtils.loadLibSVMFile(sc, file).zipWithIndex()
                                            else
                                            {
                                              val rawData=sc.textFile(file)
                                              rawData.map({ line => val values=line.split(";")
                                                                    (new LabeledPoint(0.0, Vectors.dense(values.slice(1, values.length).map { x => x.toDouble })), values(0).toLong-1)
                                                          })
                                            }
      
      /*
      //Normalize if necessary
      val maxMins=data.map({case (point, index) => (point.features.toArray, point.features.toArray)})
                      .reduce({case ((max1, min1), (max2, min2)) =>
                                var maxLength=max1.length
                                var longMax=max1
                                var longMin=min1
                                var shortMax=max2
                                var shortMin=min2
                                if (max2.length>maxLength)
                                {
                                  maxLength=max2.length
                                  longMax=max2
                                  longMin=min2
                                  shortMax=max1
                                  shortMin=min1
                                }
                                for (i <- 0 until maxLength)
                                {
                                  if (i<shortMax.length)
                                  {
                                    if (longMax(i)<shortMax(i))
                                      longMax(i)=shortMax(i)
                                    if (longMin(i)>shortMin(i))
                                      longMin(i)=shortMin(i)
                                  }
                                }
                                (longMax, longMin)
                              })
      val ranges=new Array[Double](maxMins._1.length)
      for (i <- 0 until ranges.length)
        ranges(i)=maxMins._1(i)-maxMins._2(i)
      
      val normalizedData=data.map({case (point, index) =>
                                      val feats=point.features
                                      if (feats.isInstanceOf[DenseVector])
                                      {
                                        val dense=feats.asInstanceOf[DenseVector].values
                                        for (i <- 0 until dense.size)
                                          dense(i)=(dense(i)-maxMins._2(i))/ranges(i)
                                      }
                                      else
                                      {
                                        val sparse=feats.asInstanceOf[SparseVector]
                                        val indices=sparse.indices
                                        val values=sparse.values
                                        
                                        for (i <- 0 until indices.length)
                                          values(i)=(values(i)-maxMins._2(indices(i)))/ranges(indices(i))
                                      }
                                      (point, index)
                                  })
      
      println("Normalized dataset")
      normalizedData.foreach(println(_))
      */
      
      val n=data.count()
      println("Dataset has "+n+" elements")
      
      /* GRAPH VERSION 
      
      val graph=LSHGraphXKNNGraphBuilder.getGraph(data, numNeighbors, dimension)
      println("There goes the graph:")
      graph.foreach(println(_))
      
      */
      
      val graph:RDD[(Long,List[(Long,Double)])]=if (METHOD=="lsh")
                  /* LOOKUP VERSION */
                  LSHLookupKNNGraphBuilder.computeGraph(data, numNeighbors)
                else
                  /* BRUTEFORCE VERSION */
                  BruteForceKNNGraphBuilder.parallelComputeGraph(data, numNeighbors)
      
      //Print graph
      /*println("There goes the graph:")
      graph.foreach({case (elementIndex, neighbors) =>
                      for(n <- neighbors)
                        println(elementIndex+"->"+n._1+"("+n._2+")")
                    })
      */
                  
      val edges=graph.flatMap({case (index, neighbors) => neighbors.map({case (destination, distance) => (index, destination, distance)}).toSet})
      
      var counted=edges.map({case x=>(x._1,1)}).reduceByKey(_+_).sortBy(_._1)
      //counted.foreach(println(_))
      
      var forCount=counted.map(_._2)
      println("Obtained "+forCount.sum()+" edges for "+forCount.count()+" nodes")
      
      //Save to file
      //TEMP
      val fileParts=file.split("/")
      var justFileName=fileParts(fileParts.length-1).split("\\.")(0)
      val path="/home/eirasf/Escritorio/kNNTEMP/"+justFileName
      val fileName=path+numNeighbors+"NN_graph-"+METHOD+Random.nextInt(1000)
      edges
          .sortBy(_._1) //TEMP
          .saveAsTextFile(fileName)
          
      /*
      
      //TEMP - Compare with ground truth
      CompareGraphs.compare(path+numNeighbors+"NN_graph-brute", fileName+"/part-00000")
      
      if (METHOD=="lsh")
      {
        val fileNameR=path+numNeighbors+"NN_graphRefined-"+METHOD+Random.nextInt(1000)
        val edgesR=LSHLookupKNNGraphBuilder.refineGraph(data, graph, numNeighbors)
                                           .flatMap({case (index, neighbors) =>
                                                           neighbors.map({case (destination, distance) =>
                                                                                 (index, destination, distance)}).toSet})
        edgesR
            .sortBy(_._1) //TEMP
            .saveAsTextFile(fileNameR)
            
        //TEMP - Compare with ground truth
        CompareGraphs.compare(path+numNeighbors+"NN_graph-brute", fileNameR+"/part-00000")
      }
      */
      
      //Stop the Spark Context
      sc.stop()
    }
  }