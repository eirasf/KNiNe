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
  val MIN_TOLERANCE=0.4
  val MAX_TOLERANCE=1.1
  def getHasherForDataset(data: RDD[(Long,LabeledPoint)], dimension:Int, desiredComparisons:Int):(EuclideanLSHasher,Int,Double)
  
  def getHasherForDataset(data: RDD[(Long,LabeledPoint)], desiredComparisons:Int):(EuclideanLSHasher,Int,Double)=
    getHasherForDataset(data, data.map({case (index, point) => point.features.size}).max(), desiredComparisons)
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

  private def computeBestKeyLength(data: RDD[(Long,LabeledPoint)], dimension:Int, desiredComparisons:Int): (EuclideanLSHasher,Double) = {
    val FRACTION=1.0//0.01
    val INITIAL_RADIUS=0.1
    val initialData = data//data.sample(false, FRACTION, 56804023).map(_.swap)
    
    var initialKLength: Int = Math.ceil(log2(data.count() / dimension)).toInt + 1
    if (initialKLength<1) initialKLength=1
    val minKLength=if (initialKLength>10) (initialKLength / 2).toInt else 5 
    val maxKLength=if (initialKLength>15) (initialKLength * 1.5).toInt else 22
    val hNTables: Int = Math.floor(Math.pow(log2(dimension), 2)).toInt
    
    val currentData=initialData
    //val currentData=initialData.sample(false, 0.2, 34652912) //20% of the data usually does the job.
    
    println(s"Starting hyperparameter adjusting with:\n\tL:$initialKLength\n\tN:$hNTables\n\tR:$INITIAL_RADIUS\n\tC:$desiredComparisons")
    
    var (leftLimit,rightLimit)=(minKLength,maxKLength)
    var radius = INITIAL_RADIUS
    var isRadiusAdjusted = false

    while(true)
    {
      val currentLength=Math.floor((leftLimit+rightLimit)/2.0).toInt
      val tmpHasher = new EuclideanLSHasher(dimension, currentLength, hNTables)
      val (numBuckets,largestBucketSizeSample) = getBucketCount(currentData, tmpHasher, radius)
      val largestBucketSize=largestBucketSizeSample///FRACTION
      
      if ((largestBucketSize>=desiredComparisons*MIN_TOLERANCE) && (largestBucketSize<=desiredComparisons*MAX_TOLERANCE))
      {
        println(s"Found suitable hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
        return (tmpHasher,radius)
      }
      else
      {
        if (largestBucketSize<desiredComparisons*MIN_TOLERANCE) //Buckets are too small
        {
          if ((numBuckets==0) || (rightLimit-1 == currentLength)) //If we ended up with no buckets with more than one element or the size is less than the desired minimum
          {
            if (isRadiusAdjusted)
            {
              println(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
              return (tmpHasher,radius)
            }
            //We start over with a larger the radius
            radius=getSuitableRadius(currentData, new EuclideanLSHasher(dimension, initialKLength, hNTables), radius, None, desiredComparisons)
            isRadiusAdjusted=true
            leftLimit=minKLength
            rightLimit=maxKLength
          }
          else
            rightLimit=currentLength
        }
        else //Buckets are too large
        {
          if (leftLimit == currentLength)
          {
            if (isRadiusAdjusted)
            {
              println(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
              return (tmpHasher,radius)
            }
            //We start over with a smaller the radius
            radius=getSuitableRadius(currentData, tmpHasher, 0.000000000001, Some(radius), desiredComparisons)
            isRadiusAdjusted=true
            leftLimit=minKLength
            rightLimit=maxKLength
          }
          else
            leftLimit=currentLength
        }
        if (rightLimit<=leftLimit)
        {
          println(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
          return (tmpHasher,radius)
        }
      }
      
      println(s"keyLength update to ${tmpHasher.keyLength} [$leftLimit - $rightLimit] with radius $radius because largestBucket was $largestBucketSize and wanted [${desiredComparisons*MIN_TOLERANCE} - ${desiredComparisons*MAX_TOLERANCE}]")
    }
    return (new EuclideanLSHasher(dimension, 1, hNTables), radius)//Dummy
  }
  
  def getSuitableRadius(data:RDD[(Long,LabeledPoint)], hasher:EuclideanLSHasher, minValue:Double, maxValue:Option[Double], desiredCount:Int):Double=
  {
    var leftLimit=minValue
    var rightLimit=if (maxValue.isDefined)
                     maxValue.get
                   else
                   {
                     //Find a radius that is too large
                     var done=false
                     var currentValue=leftLimit*2
                     while (!done)
                     {
                       val (numBuckets, largestBucketSize)=getBucketCount(data, hasher, currentValue)
                       done=largestBucketSize>desiredCount*2
                       println(s"Radius range updated to [$leftLimit - $currentValue] got a largest bucket of $largestBucketSize")
                       if (!done)
                         currentValue*=2
                       if ((largestBucketSize>MIN_TOLERANCE*desiredCount) && (largestBucketSize<MAX_TOLERANCE*desiredCount))
                       {
                         println(s"Found suitable radius at $currentValue")
                         return currentValue
                       }
                     }
                     currentValue
                   }
    while(true)
    {
      val radius=(leftLimit+rightLimit)/2
      val (numBuckets, largestBucketSize)=getBucketCount(data, hasher, radius)
      println(s"Radius update to $radius [$leftLimit - $rightLimit] got a largest bucket of $largestBucketSize")
      if ((largestBucketSize>=MIN_TOLERANCE*desiredCount) && (largestBucketSize<=MAX_TOLERANCE*desiredCount))
      {
        println(s"Found suitable radius at $radius")
        return radius
      }
      if ((numBuckets==0) || (largestBucketSize<MIN_TOLERANCE*desiredCount))
        leftLimit=radius
      else
        if (largestBucketSize>MIN_TOLERANCE*desiredCount)
        {
          rightLimit=radius
          /*
          //DEBUG
          
            val currentHashes = hashData(data, hasher, radius)
            var lookup:BroadcastLookupProvider=new BroadcastLookupProvider(data)
            //bucketCountBySize is a list of (bucket_size, count) tuples that indicates how many buckets of a given size there are. Count must be >1.
            val bucketCountBySize = currentHashes.groupByKey().filter({case (h,indexList) => indexList.size>1}).flatMap({case (h,indexList) => indexList.map(lookup.lookup(_))}).take(100).foreach(println)
            System.exit(0) 
          */
        }
      if (rightLimit-leftLimit<0.000000001)
      {
        println(s"WARNING! - Had to select radius = $radius")
        return radius
      }
    }
    return 1.0//Dummy
  }
  
  def getBucketCount(data:RDD[(Long,LabeledPoint)], hasher:EuclideanLSHasher, radius:Double):(Int,Int)=
  {
    val currentHashes = hashData(data, hasher, radius)
    //bucketCountBySize is a list of (bucket_size, count) tuples that indicates how many buckets of a given size there are. Count must be >1.
    val bucketCountBySize = currentHashes.aggregateByKey(0)({ case (n, index) => n + 1 }, { case (n1, n2) => n1 + n2 })
                                         .map({ case (h, n) => (n, 1) })
                                         .reduceByKey(_ + _)
                                         .filter({ case (n1, x) => n1 != 1 })

    /*DEBUG
    bucketCountBySize.sortBy(_._1)
                      .repartition(1)
                      .foreach({ case x => println(x._2 + " buckets with " + x._1 + " elements") })
    */

    
    val numBuckets = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.reduce({ case ((n1, x), (n2, y)) => (n1 + n2, x + y) })._2
    val largestBucketSize = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._1).max()
    return (numBuckets, largestBucketSize)
  }
  
  override def getHasherForDataset(data: RDD[(Long,LabeledPoint)], dimension:Int, desiredComparisons:Int):(EuclideanLSHasher,Int,Double)=
  {
    //val factorLevel=Math.pow(10,-minusLogOperations)/0.001
    //val predictedNTables: Int = Math.floor(Math.pow(log2(dimension), 2)).toInt
    //var mComparisons: Int = Math.abs(Math.ceil(hasher.numTables * Math.sqrt(log2(data.count()/(dimension*0.1))))).toInt
    //var mComparisons: Int = Math.abs(Math.ceil(predictedNTables * Math.sqrt(log2(data.count()/(dimension*0.1)*factorLevel)))).toInt
    //println(s"CMAX set to $mComparisons do approximately ${Math.pow(10,-minusLogOperations)} of the calculations wrt brute force.")
    //val (hasher,radius) = computeBestKeyLength(data, dimension, (desiredComparisons/1.5).toInt)
    val (hasher,radius) = computeBestKeyLength(data, dimension, desiredComparisons)

    println("R0:" + radius + " num_tables:" + hasher.numTables + " keyLength:" + hasher.keyLength + " desiredComparisons:" + desiredComparisons)
    //System.exit(0) //DEBUG
    return (hasher, desiredComparisons, radius)
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
      val hash=new Array[Integer](keyLength+1)
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
      hash(keyLength)=i//Add the number of table to the end of the hash to avoid collisions with hashes from other tables.
      hashes=(new Hash(hash),index) :: hashes
    }
    return hashes
  }
}
