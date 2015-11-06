package es.udc.graph.test

import org.specs2.mutable.Specification

import es.udc.graph.utils.GraphUtils._
import es.udc.graph.utils.Selection._

import scala.runtime.RichInt
import scala.util.Random

/**
 * Created by davidmartinezrego on 06/11/15.
 */
class GraphUtilsTest extends Specification{

  "medianUpTo5" should {
    "find the median of 5 elements" in {
      val medians = List(1,2,3,4,5).permutations.map{case per => medianUpTo5(per.toArray)}
      assert(medians.forall(a => a == 3))
    }
  }

  "medianUpTo5" should {
    "find the median of less than 5 elements" in {
      var medians = List(1,2,3).permutations.map{case per => medianUpTo5(per.toArray)}
      assert(medians.forall(a => a == 2))
      medians = List(1,2,3,4).permutations.map{case per => medianUpTo5(per.toArray)}
      assert(medians.forall(a => a == 2))
    }
  }

  "chooseMedianOfMedians" should {
    "find the median of a list of elements" in {
      val medians = List(1,2,3).permutations.map{case per => chooseMedianOfMedians(per.toArray)}
      assert(medians.forall(a => a == 2))
    }
  }

  "chooseMedianOfMedians" should {
    "find the median of a list of 5 elements" in {
      val medians = List(1,2,3,4,5).permutations.map{case per => chooseMedianOfMedians(per.toArray)}
      assert(medians.forall(a => a == 3))
    }
  }

  "chooseMedianOfMedians" should {
    "find the median of a list of a long list of elements" in {
      val to30 = 1 to 30
      for (i <- 1 to 200){
        val a = Random.shuffle(to30).toArray
        val median = chooseMedianOfMedians(a)
        val below = a.filter({case value => value < median}).size
        assert(below >= 8 && below <= 20)
      }

      val to100 = 1 to 100
      for (i <- 1 to 20){
        val a = Random.shuffle(to100).toArray
        val median = chooseMedianOfMedians(a)
        assert(median >= 30 && median <= 70)
      }
    }
  }

  "findKMedian" should {

    "find the kth element in a list" in {
      var medians = List(1,2,3,4,5).permutations.map{case per => findKMedian(per.toArray, 4)(chooseMedianOfMedians[Int])}
      assert(medians.forall(a => a == 4))
      medians = List(1,2,3,4,5).permutations.map{case per => findKMedian(per.toArray, 3)(chooseMedianOfMedians[Int])}
      assert(medians.forall(a => a == 3))
      medians = List(1,2,3,4,5).permutations.map{case per => findKMedian(per.toArray, 2)(chooseMedianOfMedians[Int])}
      assert(medians.forall(a => a == 2))
      medians = List(1,2,3,4,5).permutations.map{case per => findKMedian(per.toArray, 1)(chooseMedianOfMedians[Int])}
      assert(medians.forall(a => a == 1))

      val to30 = 1 to 30
      for (i <- 1 to 20){
        val i = Random.nextInt(30)+1
        val a = Random.shuffle(to30).toArray
        val ith = findKMedian(a, i)(chooseMedianOfMedians[Int])
        assert(i == ith)
      }

      val to100 = 1 to 100
      for (i <- 1 to 20){
        val i = Random.nextInt(100)+1
        val a = Random.shuffle(to100).toArray
        val ith = findKMedian(a, i)(chooseMedianOfMedians[Int])
        assert(i == ith)
      }
    }
  }
}