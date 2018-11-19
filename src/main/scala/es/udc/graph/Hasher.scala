package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

class Hash(var values: Array[Integer]) extends Serializable
{
    override val hashCode = values.deep.hashCode
    override def equals(obj:Any) = obj.isInstanceOf[Hash] && obj.asInstanceOf[Hash].values.deep == this.values.deep
    def concat(other:Hash):Hash=
    {
      return new Hash(this.values ++ other.values)
    }
    def cutLen(len: Int): Hash =
    {
      return new Hash(this.values.slice(0, len))
    }
}

trait Hasher extends Serializable
{
  def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]
}

trait AutotunedHasher extends Hasher
{
  def getHasherForDataset(data: RDD[(LabeledPoint, Long)], dimension:Int, factor:Double, startRadius:Double):(EuclideanLSHasher,Int,Double)
  
  def getHasherForDataset(data: RDD[(LabeledPoint, Long)], factor:Double, startRadius:Double):(EuclideanLSHasher,Int,Double)=
    getHasherForDataset(data, data.map({case (point, index) => point.features.size}).max(), factor, startRadius)
}

object EuclideanLSHasher extends AutotunedHasher
{
  protected final def hashData(data: RDD[(Long, LabeledPoint)], hasher: EuclideanLSHasher, radius: Double): RDD[(Hash, Long)] =
  {
    data.flatMap({ case (index, point) => hasher.getHashes(point.features, index, radius) });
  }
  
  protected def log2(n: Double): Double =
  {
    Math.log10(n) / Math.log10(2)
  }

  private def computeBestKeyLength(data: RDD[(LabeledPoint, Long)], keyLength: Int, hasher: EuclideanLSHasher, startRadius: Double): (Int, Double) = {
    val currentData = data.map(_.swap)
    var tmpHasher = hasher
    var radius = startRadius
    var tmpHash = hashData(currentData, hasher, radius)
    var update = false
    var upFlag = false
    var downFlag = false

    do {
      update = false
      if (tmpHasher.keyLength < hasher.keyLength) {
        tmpHash = tmpHash.map({ case (hash, index) =>
          (hash.cutLen(tmpHasher.keyLength), index)
        });
      } else if (tmpHasher.keyLength != hasher.keyLength) {
        tmpHash = hashData(currentData, tmpHasher, radius)
      }
      /*
            val hashBuckets = tmpHash.groupByKey()
              .map({ case (k, l) => (k, l.toSet) })
              .map({ case (k, s) => (k, s, s.size) })

            val numBuckets = hashBuckets.count()
            val stepOps = hashBuckets.map({ case (h, s, n) => (n, 1) }).reduceByKey(_ + _)
      */
      val stepOps = tmpHash.aggregateByKey(0)({ case (n, index) => n + 1 }, { case (n1, n2) => n1 + n2 })
        .map({ case (h, n) => (n, 1) }).reduceByKey(_ + _).filter({ case (n1, x) => n1 != 1 })

      val numBuckets = if (stepOps.isEmpty()) 0 else stepOps.reduce({ case ((n1, x), (n2, y)) => (n1 + n2, x + y) })._2
      val maxGroup = if (stepOps.isEmpty()) (0, 0) else stepOps.reduce({ case ((n1, x), (n2, y)) => if (x > y) (n1, x) else (n2, y) })

      //val mean = stepOps.map({ case (n1, x) => n1 * x }).reduce({ case (x1, x2) => x1 + x2 }) / numBuckets
      //val desv = Math.sqrt(stepOps.map({ case (n, x) => (x - mean) * (x - mean) * n }).reduce({ case (d1, d2) => d1 + d2 }) / numBuckets)

      if ((stepOps.isEmpty() || tmpHasher.keyLength < hasher.keyLength / 2) && !upFlag) {
        radius *= 2
        tmpHasher = new EuclideanLSHasher(tmpHasher.dim, hasher.keyLength, tmpHasher.numTables)
        tmpHash = hashData(currentData, tmpHasher, radius)
        update = true
      } else {
        //if (numBuckets * 0.5 < maxGroup._2)
        val elems = stepOps.map({ case (n1, x) => n1 })
        val mean = elems.reduce({ case (n1, n2) => n1 + n2 }) * 1.0 / elems.count()
        val desv = Math.sqrt(elems.map({ case n => (n - mean) * (n - mean) }).reduce({ case (n1, n2) => n1 + n2 }) * 1.0 / elems.count())
        val maxElems = stepOps.reduce({ case ((n1, x), (n2, y)) => if (n1 > n2) (n1, x) else (n2, y) })._1
        if (numBuckets * 0.5 < maxGroup._2 && (maxElems < mean + 3 * desv || desv == 0)) {
          tmpHasher = new EuclideanLSHasher(tmpHasher.dim, tmpHasher.keyLength - 2, tmpHasher.numTables)
          downFlag = true
          update = !upFlag
        }
        else if (!downFlag) {
          tmpHasher = new EuclideanLSHasher(tmpHasher.dim, tmpHasher.keyLength + 2, tmpHasher.numTables)
          update = true
          upFlag = true
        }
      }

      stepOps.sortBy(_._1).repartition(1)
        .foreach({ case x => println(x._2 + " buckets with " + x._1 + " elements") })

      if (update) {
        println("keyLength update to " + tmpHasher.keyLength + " with radius " + radius)
      } else {
        println("keyLength set to " + tmpHasher.keyLength + " with radius " + radius)
      }


    } while (update)
    (tmpHasher.keyLength, radius)
  }
  override def getHasherForDataset(data: RDD[(LabeledPoint, Long)], dimension:Int, factor:Double, startRadius:Double):(EuclideanLSHasher,Int,Double)=
  {
    var radius = startRadius
    var hKLength: Int = Math.ceil(log2(data.count() / dimension)).toInt + 1
    val (hK, r) = computeBestKeyLength(data, hKLength, new EuclideanLSHasher(dimension, hKLength, 1), radius)
      hKLength = hK
      radius = r
    var hNTables: Int = Math.floor(Math.pow(log2(dimension), 2) * factor).toInt
    var mComparisons: Int = Math.abs(Math.ceil(hNTables * Math.sqrt(factor) * Math.sqrt(log2(data.count()/(dimension*0.1))))).toInt

    println("R0:" + radius + " num_tables:" + hNTables + " keyLength:" + hKLength + " maxComparisons:" + mComparisons)
    return (new EuclideanLSHasher(dimension, hKLength, hNTables), mComparisons, radius)
  }
  
  override def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]=
  {
    return List() //Workaround
  }
}

class EuclideanLSHasher(dimension:Int, kLength:Int, nTables:Int) extends Hasher 
{
  private val OptimalW=4
  val numTables=nTables
  val keyLength=kLength
  val w=OptimalW
  val dim=dimension
  
  val gaussianVectors=ofDim[Double](numTables, keyLength, dimension)
  val b=ofDim[Double](numTables, keyLength)
  
  protected def _init():Unit=
  {
    val randomGenerator=new Random()
    for(i <- 0 until numTables)
      for (j <- 0 until keyLength)
      {
        for (k <- 0 until dimension)
          gaussianVectors(i)(j)(k)=randomGenerator.nextGaussian()
        b(i)(j)=randomGenerator.nextDouble*w
      }
  }
  this._init()
  
  override def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]=
  {
    var hashes=List[(Hash, Long)]()
    for(i <- 0 until numTables)
    {
      val hash=new Array[Integer](keyLength)
      for (j <- 0 until keyLength)
      {
        var dotProd:Double=0
        //TODO Take dot product to a function or use a prebuilt one
        if (point.isInstanceOf[DenseVector])
        {
          for (k <- 0 until dimension)
            dotProd+=point(k) * gaussianVectors(i)(j)(k)
        }
        else //SparseVector
        {
          val sparse=point.asInstanceOf[SparseVector]
          val indices=sparse.indices
          val values=sparse.values
          
          for (k <- 0 until indices.length)
          {
            //if (indices(k)>=dimension)
            //  break
            if (indices(k)<dimension)
              dotProd+=values(k) * gaussianVectors(i)(j)(indices(k))
          }
        }
        dotProd/=radius
        hash(j)=math.floor((dotProd + b(i)(j))/w).toInt
      }
      hashes=(new Hash(hash),index) :: hashes
    }
    return hashes
  }
}