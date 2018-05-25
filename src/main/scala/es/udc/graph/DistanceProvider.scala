package es.udc.graph

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

trait DistanceProvider extends Serializable
{
  def getDistance(p1:LabeledPoint,p2:LabeledPoint):Double;
}

class EuclideanDistanceProvider() extends DistanceProvider
{
  def getDistance(p1:LabeledPoint,p2:LabeledPoint):Double=
  {
    return Vectors.sqdist(p1.features, p2.features)
  }
}
