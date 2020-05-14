package es.udc.graph.utils

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import Selection.chooseMedianOfMedians
import Selection.findKMedian
import es.udc.graph.Neighbor
import es.udc.graph.sparkContextSingleton
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner
import es.udc.graph.GraphMerger
import es.udc.graph.IndexDistancePair
import es.udc.graph.NeighborsForElement

object TruncateGraph
{
  def showUsageAndExit()=
  {
    println("""Usage: TruncateGraph graphFile""")
    System.exit(-1)
  }
  def parseParams(p:Array[String]):Map[String, Any]=
  {
    val m=scala.collection.mutable.Map[String, Any]()
    if (p.length<1)
      showUsageAndExit()
    
    m("dataset")=p(0)
    
    return m.toMap
  }
  def main(args: Array[String])
  {
    if (args.length <= 0)
    {
      showUsageAndExit()
      return
    }
    
    val options=parseParams(args)
    val numPartitions=512
    val datasetFile=options("dataset").asInstanceOf[String]
    
    
    //Set up Spark Context
    val sc=sparkContextSingleton.getInstance()
    println(s"Default parallelism: ${sc.defaultParallelism}")
    //Stop annoying INFO messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    
    //Load data from file
    val data = sc.textFile(datasetFile,numPartitions)
                                                        .map({ line => val values=line.substring(1,line.length-1).split(",")
                                                                  val neighs=new NeighborsForElement(128)
                                                                  neighs.addElement(values(1).toLong, values(2).toDouble)
                                                                  (values(0).toLong, neighs)
                                                        }).partitionBy(new HashPartitioner(numPartitions))
                                                        .reduceByKey({case (l1,l2) => NeighborsForElement.merge(l1,l2)})
                                                        .sortBy(_._1, true, numPartitions)
    
    for (k <- List(2,4,8,16,32,64,128))
    {
      data.flatMap({case (id,neighbors) => neighbors.listNeighbors.takeRight(k).map({case pair => (id,pair.index,pair.distance)})}).saveAsTextFile(datasetFile.replace("128", ""+k))
      println(s"Saved ${k}-NN graph in "+datasetFile.replace("128_", ""+k))
    }
    //Stop the Spark Context
    sc.stop()
  }
}
