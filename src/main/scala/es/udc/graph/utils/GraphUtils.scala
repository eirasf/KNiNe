package es.udc.graph.utils

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import Selection._

/**
 * Created by david martinez rego on 06/11/15.
 */
object GraphUtils {

  case class Peer(id:Long, dist:Double) extends Ordered[Peer] {
    override def compare(that: Peer): Int = {
      return (dist - that.dist).toInt
    }
  }

  def combineNeighbors(k: Int)(l1 : Array[Peer])(l2 : Array[Peer]): Array[Peer] = {
    val fusioned = l1++l2
    val median = findKMedian(fusioned, k)(chooseMedianOfMedians[Peer])
    fusioned.filter(el => el <= median)
  }

  def calculateNearest(data : RDD[(LabeledPoint, Long)], k: Int,
                       distance : (LabeledPoint, LabeledPoint) => Double,
                       connections : RDD[Pair[Long, Long]]) = {
    val vertices = data.map{case a => a.swap}
    val edges = connections.map{case (a, b) => Edge(a, b, 0)}
    val graph = Graph(vertices, edges)
    graph.aggregateMessages[Array[Peer]](ctx => {val dist = distance(ctx.srcAttr, ctx.dstAttr); ctx.sendToDst(Array(Peer(ctx.srcId, dist)))},
      combineNeighbors(k)(_)(_))
  }

}
