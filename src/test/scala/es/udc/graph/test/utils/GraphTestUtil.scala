package com.zuhlke.test.utils

import org.apache.spark.graphx._
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by David Martinez Rego on 11/06/15.
 */
object GraphTestUtil {

  def extractEdges(edges : EdgeRDD[Double], id1 : VertexId, id2 : VertexId) : Array[Edge[Double]] = {
    edges.filter(edge => edge.srcId == id1 && edge.dstId == id2).collect()
  }

  def extractVertices(vertices : VertexRDD[LabeledPoint], id : VertexId) : Array[Pair[VertexId, LabeledPoint]] = {
    vertices.filter{case (point, idp) => idp == id}.collect()
  }
}