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
  private val conf : SparkConf = new SparkConf().setAppName("KNiNe")
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
  val DEFAULT_METHOD="lsh"
  val DEFAULT_K=10
  
  def showUsageAndExit()=
  {
    println("""Usage: KNiNe dataset output_file [options]
    Dataset must be a libsvm or text file
Options:
    -k    Number of neighbors (default: """+KNiNe.DEFAULT_K+""")
    -m    Method used to compute the graph. Valid values: lsh, brute (default: """+KNiNe.DEFAULT_METHOD+""")
    -r    Starting radius (default: """+LSHKNNGraphBuilder.DEFAULT_RADIUS_START+""")
    -n    Number of hashes per item (default: """+Hasher.DEFAULT_NUM_TABLES+""")
    -l    Hash length (default: """+Hasher.DEFAULT_KEY_LENGTH+""")
    -t    Maximum comparisons per item (default: auto)
    -c    File containing the graph to compare to (default: nothing)""")
    System.exit(-1)
  }
  def parseParams(p:Array[String]):Map[String, Any]=
  {
    val m=scala.collection.mutable.Map[String, Any]("num_neighbors" -> KNiNe.DEFAULT_K.toDouble,
                                                    "method" -> KNiNe.DEFAULT_METHOD,
                                                    "radius_start" -> LSHKNNGraphBuilder.DEFAULT_RADIUS_START,
                                                    "num_tables" -> Hasher.DEFAULT_NUM_TABLES.toDouble,
                                                    "key_length" -> Hasher.DEFAULT_KEY_LENGTH.toDouble)
    if (p.length<=1)
      showUsageAndExit()
    
    m("dataset")=p(0)
    m("output")=p(1)
    
    var i=2
    while (i < p.length)
    {
      if ((i>=p.length-1) || (p(i).charAt(0)!='-'))
      {
        println("Unknown option: "+p(i))
        showUsageAndExit()
      }
      val readOptionName=p(i).substring(1)
      val option=readOptionName match
        {
          case "k"   => "num_neighbors"
          case "m"   => "method"
          case "r"   => "radius_start"
          case "n"   => "num_tables"
          case "l"   => "key_length"
          case "t"   => "max_comparisons"
          case "c"   => "compare"
          case somethingElse => readOptionName
        }
      if (!m.keySet.exists(_==option) && option==readOptionName)
      {
        println("Unknown option:"+readOptionName)
        showUsageAndExit()
      }
      if (option=="method")
      {
        if (p(i+1)=="lsh" || p(i+1)=="brute")
          m(option)=p(i+1)
        else
        {
          println("Unknown method:"+p(i+1))
          showUsageAndExit()
        }
      }
      else
      {
        if (option=="compare")
          m(option)=p(i+1)
        else
          m(option)=p(i+1).toDouble
      }
        
      i=i+2
    }
    return m.toMap
  }
  def main(args: Array[String])
  {
    if (args.length <= 0)
    {
      println("An input libsvm file must be provided")
      return
    }
    
    val options=parseParams(args)
    
    val datasetFile=options("dataset").asInstanceOf[String]
    
    val fileParts=datasetFile.split("/")
    var justFileName=fileParts(fileParts.length-1).split("\\.")(0)
//val file="/home/eirasf/Escritorio/kNNTEMP/car-dopado.libsvm"
    val numNeighbors=options("num_neighbors").asInstanceOf[Double].toInt
    val method=options("method")
    val format=if ((datasetFile.length()>7) && (datasetFile.substring(datasetFile.length()-7) ==".libsvm"))
                 "libsvm"
               else
                 "text"
                 
    val radius0=options("radius_start").asInstanceOf[Double]
    val numTables=options("num_tables").asInstanceOf[Double].toInt
    val keyLength=options("key_length").asInstanceOf[Double].toInt
    val compareFile=if (options.exists(_._1=="compare"))
                      options("compare").asInstanceOf[String]
                    else
                      null
    val maxComparisons:Int=if (options.exists(_._1=="max_comparisons"))
                         options("max_comparisons").asInstanceOf[Double].toInt
                       else
                         -1
                 
//    println("Using "+method+" to compute "+numNeighbors+"NN graph for dataset "+justFileName)
//    println("R0:"+radius0+" num_tables:"+numTables+" keyLength:"+keyLength+(if (maxComparisons!=null)" maxComparisons:"+maxComparisons else ""))
    
    //Set up Spark Context
    val sc=sparkContextSingleton.getInstance()
    
    //Stop annoying INFO messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    
    //Load data from file
    val data: RDD[(LabeledPoint, Long)] = if (format=="libsvm")
                                            MLUtils.loadLibSVMFile(sc, datasetFile).zipWithIndex()
                                          else
                                          {
                                            val rawData=sc.textFile(datasetFile)
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
    
    //val n=data.count()
    //println("Dataset has "+n+" elements")
    
    /* GRAPH VERSION 
    
    val graph=LSHGraphXKNNGraphBuilder.getGraph(data, numNeighbors, dimension)
    println("There goes the graph:")
    graph.foreach(println(_))
    
    */
    
    val graph:RDD[(Long,List[(Long,Double)])]=if (method=="lsh")
                                                /* LOOKUP VERSION */
                                                LSHLookupKNNGraphBuilder.computeGraph(data, numNeighbors, keyLength, numTables, radius0, maxComparisons)
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
    
    //DEBUG
    var counted=edges.map({case x=>(x._1,1)}).reduceByKey(_+_).sortBy(_._1)
    var forCount=counted.map(_._2)
    //println("Obtained "+forCount.sum()+" edges for "+forCount.count()+" nodes")
    
    //Save to file
    var fileName=options("output").asInstanceOf[String]
    var fileNameOriginal=fileName
    var i=0
    while (java.nio.file.Files.exists(java.nio.file.Paths.get(fileName)))
    {
      i=i+1
      fileName=fileNameOriginal+"-"+i
    }
    edges
        .sortBy(_._1) //TEMP
        .saveAsTextFile(fileName)
        
    /*DEBUG*/
    
    if (compareFile!=null)
    {
      //TEMP - Compare with ground truth
      var firstComparison=CompareGraphs.compare(compareFile, fileName+"/part-00000")
      
      /*if (method=="lsh")
      {
        var refinedGraph=graph
        for (i <- 0 until 1)
        {
          println("Refined "+i)
          refinedGraph=LSHLookupKNNGraphBuilder.refineGraph(data, refinedGraph, numNeighbors)
          val fileNameR=fileName+"refined"+i
          val edgesR=refinedGraph.flatMap({case (index, neighbors) =>
                                                   neighbors.map({case (destination, distance) =>
                                                                         (index, destination, distance)}).toSet})
          edgesR
              .sortBy(_._1) //TEMP
              .saveAsTextFile(fileNameR)
              
          //TEMP - Compare with ground truth
          var secondComparison=CompareGraphs.compare(compareFile, fileNameR+"/part-00000")
          
          /* //DEBUG - Show how the graph has improved
          firstComparison.join(secondComparison)
                         .flatMap({case (element,((a,b,furthest,list), (a2,b2,furthest2,list2))) => if (b!=b2 || list!=list2)
                                                                                                      Some(element, b.diff(b2), b2.diff(b))
                                                                                                    else
                                                                                                      None})
                         .sortBy(_._1)
                         .foreach(println(_))
          */
        }
      }*/
    }
    /**/
    //Stop the Spark Context
    sc.stop()
  }
}