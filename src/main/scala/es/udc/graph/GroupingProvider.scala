package es.udc.graph

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

trait GroupingProvider extends Serializable
{
  def getGroupId(p1:LabeledPoint):Int;
  def getGroupIdList():Iterable[Int]
  def numGroups:Int;
}

class DummyGroupingProvider() extends GroupingProvider
{
  val DEFAULT_GROUPID=0
  def getGroupId(p1:LabeledPoint):Int=
  {
    return DEFAULT_GROUPID
  }
  def getGroupIdList():Iterable[Int]=
  {
    return Iterable[Int](0)
  }
  def numGroups:Int=1
}
