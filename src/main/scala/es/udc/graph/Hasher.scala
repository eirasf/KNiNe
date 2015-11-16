package es.udc.graph

import breeze.numerics._
import es.udc.graph.utils.MultivariateGaussian
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.{DenseVector => BDV}

case class Hash(values: Array[Int], id : Int) extends Serializable {
    override val hashCode = values.deep.hashCode
    override def equals(obj:Any) = obj.isInstanceOf[Hash] &&
      obj.asInstanceOf[Hash].values.deep == this.values.deep &&
      this.id == obj.asInstanceOf[Hash].id
}

trait Hasher extends Serializable {
  def getHashes(point:Vector, radius:Double):List[Hash]
}

class EuclideanHasher(dim : Int, prob : Double = 0.9, seed : Int = 1) extends Hasher {

  val (l, k, w) = getOptimalParameters(prob)

  val sampler = MultivariateGaussian(dim, seed)
  val proyectors = List.fill(l)(sampler.draw(k))
  val b = List.fill(l, k)(math.random * w)


  // TODO: Implement the calculation of a valid L and k for given probability
  def getOptimalParameters(prob : Double) : Triple[Int, Int, Double] = {
    val w=4.0 // This value has been researched thoroughly and 4 seems to be good for practical purposes
    (4, 5, w)
  }

  override def getHashes(point : Vector, R:Double): List[Hash]= {
    proyectors.zipWithIndex.zip(b).map{ case hash =>
        val W = hash._1._1
        val tabId = hash._1._2
        val b = new BDV[Double](hash._2.toArray)
        val p = new BDV[Double](point.toArray.map(_/R))
        val vals =  floor((b + (W*p) /= w))
        Hash(vals.toArray.map(_.toInt), tabId)
    }
  }
}