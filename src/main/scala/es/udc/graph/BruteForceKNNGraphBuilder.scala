package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector

class BruteForceKNNGraphBuilder(pNumNeighbors:Int)
{
  private val numNeighbors=pNumNeighbors
  
  class IndexDistancePair(pIndex: Int, pDistance: Double)
  {
    var index: Int = pIndex
    var distance: Double = pDistance
  }
  
  class NeighborsForElement()
  {
    var maxDistance=Double.MinValue
    var listNeighbors=List[IndexDistancePair]()
    
    def addElement(index:Int, distance:Double):Unit=
    {
      if (listNeighbors.size<numNeighbors)
         {
           if (maxDistance<distance)
             maxDistance=distance
           listNeighbors=new IndexDistancePair(index, distance) :: listNeighbors 
         }
         else //Already have enough neighbors
         {
           if (maxDistance>distance) //Only update if this one is closer than the ones we already have
           {
             //Loop once through the existing neighbors replacing the one that was farthest with the new one and, at the same time, updating the maximum distance
             var maxDistanceFound=Double.MinValue
             var isInserted=false
             for (n <- listNeighbors)
             {
               if (!isInserted)
               {
                 if (n.distance==maxDistance)
                 {
                   n.index=index
                   n.distance=distance
                   isInserted=true
                 }
               }
               if (n.distance>maxDistanceFound)
                 maxDistanceFound=n.distance
             }
             maxDistance=maxDistanceFound
           }
         }
    }
  }
  
  def computeGraph(arrayIndices:Array[Long]/*Some sort of lookup, either function, structure to join, or table. Or have the points in the first parameter*/):(Int, List[(Int, Double)])=
  {
    val closestNeighbors=new Array[NeighborsForElement](arrayIndices.length) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
    //TODO Create class instead of tuple to be able to reassign
    //The computed distances could be stored elsewhere so that there is no symmetric repetition
    for(i <- 0 until arrayIndices.length-1)
      closestNeighbors(i)=new NeighborsForElement()
    for(i <- 0 until arrayIndices.length-1)
      for(j <- i+1 until arrayIndices.length)
      {
         //TODO Retrieve elements from indices and compute distance
         val d:Double=10.0
         closestNeighbors(i).addElement(j, d)
         closestNeighbors(j).addElement(i, d)
       }
     //TODO Unwrap the structure into graph edges and whatever more information is useful (distances, number of elements hit).
    //return para que no se ponga nervioso
    
    (1, List[(Int, Double)]())
  }
  
  private def addNeighbor():Unit={}
}