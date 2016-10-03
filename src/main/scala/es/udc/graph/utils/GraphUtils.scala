package es.udc.graph.utils

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import Selection._

import es.udc.graph.Neighbor

/**
 * Created by david martinez rego on 06/11/15.
 */
object GraphUtils {



  def combineNeighbors(k: Int)(l1 : List[Neighbor])(l2 : List[Neighbor]): List[Neighbor] = {
    val fusioned = l1++l2
    val median = findKMedian(fusioned.toArray, k)(chooseMedianOfMedians[Neighbor])
    fusioned.filter(el => el <= median)
  }

  def calculateNearest(data : RDD[(LabeledPoint, Long)], k: Int,
                       distance : (LabeledPoint, LabeledPoint) => Double,
                       connections : RDD[Pair[Long, Long]]) : RDD[Pair[Long, List[Neighbor]]] = {
    val vertices = data.map{case a => a.swap}
    val edges = connections.map{case (a, b) => Edge(a, b, 0)}
    val graph = Graph(vertices, edges)
    graph.aggregateMessages[List[Neighbor]](ctx => {val dist = distance(ctx.srcAttr, ctx.dstAttr);
                                                    ctx.sendToDst(List(Neighbor(ctx.srcId, dist)))
                                                    },
                                                    combineNeighbors(k)(_)(_)
                                           ).map {case a => (a._1, a._2)}
  }

}
