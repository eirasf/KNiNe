package es.udc.graph

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

trait GroupingProvider extends Serializable
{
  def getGroupId(p1:LabeledPoint):Int;
  def numGroups:Int;
}

class DummyGroupingProvider() extends GroupingProvider
{
  def getGroupId(p1:LabeledPoint):Int=
  {
    return 0
  }
  def numGroups:Int=1
}
