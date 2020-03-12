package es.udc.graph

import Array._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.internal.Logging

class Hash(var values: Array[Int]) extends Serializable
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
  val DEFAULT_RADIUS=0.1
  def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]
  def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Hash, Long)]
}

trait AutotunedHasher extends Hasher
{
  val MIN_TOLERANCE=0.4
  val MAX_TOLERANCE=1.1
  def getHasherForDataset(data: RDD[(Long,LabeledPoint)], dimension:Int, desiredComparisons:Int):(EuclideanLSHasher,Int,Double)
  
  def getHasherForDataset(data: RDD[(Long,LabeledPoint)], desiredComparisons:Int):(EuclideanLSHasher,Int,Double)=
    getHasherForDataset(data, data.map({case (index, point) => point.features.size}).max(), desiredComparisons)
}

object EuclideanLSHasher extends AutotunedHasher with Logging
{
  protected def log2(n: Double): Double =
  {
    Math.log10(n) / Math.log10(2)
  }

  private def computeBestKeyLength(data: RDD[(Long,LabeledPoint)], dimension:Int, desiredComparisons:Int): (EuclideanLSHasher,Double) = {
    val FRACTION=1.0//0.01
    val INITIAL_RADIUS=DEFAULT_RADIUS
    val initialData = data//data.sample(false, FRACTION, 56804023).map(_.swap)
    
    val numElems=data.count()
    var initialKLength: Int = Math.ceil(log2(numElems / dimension)).toInt + 1
    if (initialKLength<2) initialKLength=2
    logDebug(s"DEBUG: numElems=$numElems dimension=$dimension initialKLength=$initialKLength")
    val minKLength=if (initialKLength>10) (initialKLength / 2).toInt else 5 
    val maxKLength=if (initialKLength>15) (initialKLength * 1.5).toInt else 22
    val hNTables: Int = Math.floor(Math.pow(log2(dimension), 2)).toInt
    
    val currentData=initialData
    //val currentData=initialData.sample(false, 0.2, 34652912) //20% of the data usually does the job.
    
    logDebug(s"Starting hyperparameter adjusting with:\n\tL:$initialKLength\n\tN:$hNTables\n\tR:$INITIAL_RADIUS\n\tC:$desiredComparisons")
    
    var (leftLimit,rightLimit)=(minKLength,maxKLength)
    var radius = INITIAL_RADIUS
    var isRadiusAdjusted = false

    while(true)
    {
      val currentLength=Math.floor((leftLimit+rightLimit)/2.0).toInt
      val tmpHasher = new EuclideanLSHasher(dimension, currentLength, hNTables)
      val (numBuckets,largestBucketSizeSample) = tmpHasher.getBucketCount(currentData, radius)
      val largestBucketSize=largestBucketSizeSample///FRACTION
      
      if ((largestBucketSize>=desiredComparisons*MIN_TOLERANCE) && (largestBucketSize<=desiredComparisons*MAX_TOLERANCE))
      {
        logDebug(s"Found suitable hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
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
              logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
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
              logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
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
          logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpHasher.keyLength}\n\tN:${tmpHasher.numTables}\n\tR:$radius")
          return (tmpHasher,radius)
        }
      }
      
      logDebug(s"keyLength update to ${tmpHasher.keyLength} [$leftLimit - $rightLimit] with radius $radius because largestBucket was $largestBucketSize and wanted [${desiredComparisons*MIN_TOLERANCE} - ${desiredComparisons*MAX_TOLERANCE}]")
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
                       val (numBuckets, largestBucketSize)=hasher.getBucketCount(data, currentValue)
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
      val (numBuckets, largestBucketSize)=hasher.getBucketCount(data, radius)
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
  
  override def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Hash, Long)] =
  {
    val t=this
    val bt=data.sparkContext.broadcast(t)
    return data.flatMap({ case (index, point) => bt.value.getHashes(point.features, index, radius) });
  }
}

class EuclideanLSHasher(dimension:Int, kLength:Int, nTables:Int, splitW:Double=4.0) extends Hasher
{
  val numTables=nTables
  val keyLength=kLength
  var w=splitW
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
    //println(s"DEBUG: Getting $numTables tables of length $keyLength hashes")
    var hashes=List[(Hash, Long)]()
    for(i <- 0 until numTables)
    {
      val hash=new Array[Int](keyLength+1)
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
        	//TODO - Check if this if is needed or can be removed.
            //if (indices(k)<dimension)
              dotProd+=values(k) * gaussianVectors(i)(j)(indices(k))
          }
        }
        dotProd/=radius
        hash(j)=math.ceil((dotProd + b(i)(j))/w).toInt //Ceil ensures at least two buckets regardless of how large W is.
      }
      hash(keyLength)=i//Add the number of table to the end of the hash to avoid collisions with hashes from other tables.
      hashes=(new Hash(hash),index) :: hashes
    }
    return hashes
  }
  final def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Hash, Long)] =
  {
    val t=this
    val bt=data.sparkContext.broadcast(t)
    //println("DEBUG: Pre hashData flatMap call")
    data.flatMap({ case (index, point) => bt.value.getHashes(point.features, index, radius) });
  }
  def getBucketCount(data:RDD[(Long,LabeledPoint)], radius:Double):(Long,Int)=
  {
    //println("DEBUG: Pre hashData call")
    val currentHashes = this.hashData(data, radius)
    //println("DEBUG: Post hashData call")
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

    
    //val numBuckets = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.reduce({ case ((n1, x), (n2, y)) => (n1 + n2, x + y) })._2
    val numBuckets = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._2).sum().toLong
    val largestBucketSize = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._1).max()
    return (numBuckets, largestBucketSize)
  }
}

class EuclideanProjectedLSHasher(dimension:Int, kLength:Int, nTables:Int, blockSz:Int) extends Hasher
{
  val underlyingHasher=new EuclideanLSHasher(dimension, kLength, nTables, 1000) //FastKNN only contemplates binary hashes
  val w=ofDim[Double](kLength+1)
  
  protected def _init():Unit=
  {
    val randomGenerator=new Random()
    for (j <- 0 until underlyingHasher.keyLength)
      w(j)=randomGenerator.nextGaussian()
  }
  this._init()
  
  //Should not be used directly. This is poorly structured, but it is just a quick hack to get SimpleLSHKNNGraphBuilder working.
  override def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]=
  {
    return underlyingHasher.getHashes(point, index, radius)
  }
  final override def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Hash, Long)] =
  {
    val t=this
    val bt=data.sparkContext.broadcast(t)
    //println("DEBUG: Pre hashData flatMap call")
    val hashes=data.flatMap({ case (index, point) => bt.value.getHashes(point.features, index, radius) });
    
    return hashes.map({case (h,id) =>
                        (h.values.zip(w).map({case (hi,wi) => hi*wi}).sum,id)})
                        .sortBy(_._1)
                        .zipWithIndex
                        .map({case ((proj,id),pos) => (new Hash(Array((pos/blockSz.toLong).toInt)),id)})
  }
}

class PrecomputedProjectedLSHasher(kLength:Int, blockSz:Int) extends Hasher
{
  val w=ofDim[Double](kLength+1)
  
  protected def _init():Unit=
  {
    val randomGenerator=new Random()
    for (j <- 0 until kLength)
      w(j)=randomGenerator.nextGaussian()
  }
  this._init()
  
  //Should not be used directly. This is poorly structured, but it is just a quick hack to get SimpleLSHKNNGraphBuilder working.
  override def getHashes(point:Vector, index:Long, radius:Double):List[(Hash, Long)]=
  {
    return List() //Workaround
  }
  
  private def toBinary(n:Int):Array[Int]=
  {
    val v=Array.fill(kLength)(0)
    var rest=n
    for (i <- kLength-1 to 0 by -1)
    {
      val p=math.pow(2, i).toInt
      if (rest>=p)
      {
        v(i)=1
        rest=rest-p
      }
    }
    return v
  }
  
  final override def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Hash, Long)] =
  {
    val t=this
    val bt=data.sparkContext.broadcast(t)
    //println("DEBUG: Pre hashData flatMap call")
    val hashes=data.flatMap({ case (index, point) => List((new Hash(toBinary(point.label.toInt)), index)) });
    
    return hashes.map({case (h,id) =>
                        (h.values.zip(w).map({case (hi,wi) => hi*wi}).sum,id)})
                        .sortBy(_._1)
                        .zipWithIndex
                        .map({case ((proj,id),pos) => (new Hash(Array((pos/blockSz.toLong).toInt)),id)})
  }
}
