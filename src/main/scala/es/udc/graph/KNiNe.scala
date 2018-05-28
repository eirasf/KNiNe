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

import sys.process._


object sparkContextSingleton
{
  @transient private var instance: SparkContext = _
  private val conf : SparkConf = new SparkConf().setAppName("KNiNe")
                                                //.setMaster("local[4]")
                                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                                                .set("spark.kryoserializer.buffer.max", "512")
                                                .set("spark.driver.maxResultSize", "2048")

  def getInstance(): SparkContext=
  {
    if (instance == null)
      instance = SparkContext.getOrCreate(conf)//new SparkContext(conf)
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
    -t    Maximum comparisons per item (default: auto)
    -c    File containing the graph to compare to (default: nothing)

Advanced LSH options:
    -n    Number of hashes per item (default: auto)
    -l    Hash length (default: auto)""")
    System.exit(-1)
  }
  def parseParams(p:Array[String]):Map[String, Any]=
  {
    val m=scala.collection.mutable.Map[String, Any]("num_neighbors" -> KNiNe.DEFAULT_K.toDouble,
                                                    "method" -> KNiNe.DEFAULT_METHOD,
                                                    "radius_start" -> LSHKNNGraphBuilder.DEFAULT_RADIUS_START)
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
    val numTables=if (options.exists(_._1=="num_tables"))
                      Some(options("num_tables").asInstanceOf[Double].toInt)
                    else
                      None
    val keyLength=if (options.exists(_._1=="key_length"))
                      Some(options("key_length").asInstanceOf[Double].toInt)
                    else
                      None
    val compareFile=if (options.exists(_._1=="compare"))
                      options("compare").asInstanceOf[String]
                    else
                      null
    val maxComparisons:Int=if (options.exists(_._1=="max_comparisons"))
                         options("max_comparisons").asInstanceOf[Double].toInt
                       else
                         -1
                 
    //println("Using "+method+" to compute "+numNeighbors+"NN graph for dataset "+justFileName)
    //println("R0:"+radius0+(if (numTables!=null)" num_tables:"+numTables else "")+(if (keyLength!=null)" keyLength:"+keyLength else "")+(if (maxComparisons!=null)" maxComparisons:"+maxComparisons else ""))
    
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
    
    /* DATASET INSPECTION - DEBUG
    val summary=data.map({case x => (x._1.features.toArray,x._1.features.toArray,x._1.features.toArray)}).reduce({case ((as,aM,am),(bs,bM,bm)) => (as.zip(bs).map({case (ea,eb) => ea+eb}),aM.zip(bM).map({case (ea,eb) => Math.max(ea,eb)}),am.zip(bm).map({case (ea,eb) => Math.min(ea,eb)}))})
    val total=data.count()
    val medias=summary._1.map({ x => x/total })
    val spans=summary._2.zip(summary._3).map({case (a,b) => (a-b)})
    println(Vectors.dense(medias))
    println(Vectors.dense(spans))
    val stddevs=data.map(_._1.features.toArray.zip(medias).map({case (x,u) => (x-u)*(x-u) })).reduce({case (a,b) => a.zip(b).map({case (ea,eb) => ea+eb})}).map({ x => Math.sqrt(x/total) })
    println(Vectors.dense(stddevs))
    println(stddevs.max)
    println(stddevs.min)
    println(stddevs.sum/stddevs.length)
    System.exit(0)
    */
    
    //val n=data.count()
    //println("Dataset has "+n+" elements")
    
    /* GRAPH VERSION 
    
    val graph=LSHGraphXKNNGraphBuilder.getGraph(data, numNeighbors, dimension)
    println("There goes the graph:")
    graph.foreach(println(_))
    
    */
val timeStart=System.currentTimeMillis();    
    val (graph,lookup)=if (method=="lsh")
                                                /* LOOKUP VERSION */
                                                LSHLookupKNNGraphBuilder.computeGraph(data, numNeighbors, keyLength, numTables, radius0, maxComparisons, new EuclideanDistanceProvider())
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
    println("Obtained "+forCount.sum()+" edges for "+forCount.count()+" nodes in "+(System.currentTimeMillis()-timeStart)+" milliseconds")
    
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
        //.sortBy(_._1) //TEMP
        .saveAsTextFile(fileName)
        
    /*DEBUG*/
    
    if (compareFile!=null)
    {
      //TEMP - Compare with ground truth
      var result=getFullResultFile(fileName)
      var firstComparison=CompareGraphs.compare(compareFile, result)
      CompareGraphs.comparePositions(compareFile.replace(numNeighbors+"", "128"), result)
      
      if (method=="lsh")
      {
        var refinedGraph=graph.map({case (v, listNeighs) => (v, (0, listNeighs))})
        for (i <- 0 until 1)
        {
          println("Refined "+i)
val timeStartR=System.currentTimeMillis();          
          refinedGraph=LSHLookupKNNGraphBuilder.refineGraph(data, refinedGraph, numNeighbors, new EuclideanDistanceProvider())
          val fileNameR=fileName+"refined"+i
          val edgesR=refinedGraph.flatMap({case (index, (c,neighbors)) =>
                                                   neighbors.map({case (destination, distance) =>
                                                                         (index, destination, distance)}).toSet})
val totalElements=data.count()
val e=edgesR.first()
println("Added "+(System.currentTimeMillis()-timeStartR)+" milliseconds")
          
          edgesR
              //.sortBy(_._1) //TEMP
              .saveAsTextFile(fileNameR)
              
          //TEMP - Compare with ground truth
          result=getFullResultFile(fileNameR)
          var secondComparison=CompareGraphs.compare(compareFile, result)
          CompareGraphs.comparePositions(compareFile.replace(numNeighbors+"", "128"), result)
          
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
      }
    }
    /**/
    //Stop the Spark Context
    sc.stop()
  }
  
  def getFullResultFile(fileName:String):String=
  {
    //println("Executing /home/eirasf/Escritorio/kNNTEMP/joinParts.sh "+fileName)
    ("/home/eirasf/Escritorio/datasets-kNN/scripts/joinParts.sh "+fileName).!
    return fileName+"/result"
  }
}