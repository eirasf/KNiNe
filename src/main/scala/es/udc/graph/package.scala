package es.udc

/**
 * Created by davidmartinezrego on 11/11/15.
 */
package object graph {

  case class Neighbor(id:Long, dist:Double) extends Ordered[Neighbor] {
    override def compare(that: Neighbor): Int = {
      return (dist - that.dist).toInt
    }
  }

}
