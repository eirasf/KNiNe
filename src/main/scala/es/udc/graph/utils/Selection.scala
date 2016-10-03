package es.udc.graph.utils

import scala.annotation.tailrec
import scala.math._
import scala.reflect.ClassTag

/**
 * Implementation of the selection of the kth smallest element and k smallest elements
 * This implementation runs in linear time worst case if the Blum et. al implementation
 * is chosen
 *
 * val choosePivot = chooseMedianOfMedians[T] _
 *
 * or in average time with a smaller constant (quadratic worst time) if random pivot is selected
 *
 * val choosePivot = chooseRandomPivot[T] _
 *
 * Created by david martinez rego on 06/11/15.
 */
object Selection {

  @tailrec def findKMedian[T <% Ordered[T]](arr: Array[T], k: Int)(choosePivot: Array[T] => T)(implicit m: ClassTag[T]): T = {
    val a      = choosePivot(arr)
    val (s, b) = arr partition (_ < a)

    if (s.isEmpty) {
      val (s, b) = arr partition (a ==)
      if (s.size >= k) a
      else findKMedian(b, k - s.size)(choosePivot)
    } else if (s.size == k-1){
      return a
    } else if (k <= s.size) {
      findKMedian(s, k)(choosePivot)
    } else {
      findKMedian(b, k - s.size)(choosePivot)
    }
  }

  def findMedian[T <% Ordered[T]](arr: Array[T])(choosePivot: Array[T] => T)(implicit m: ClassTag[T]) =
    findKMedian(arr, (arr.size - 1) / 2)(choosePivot)

  // SECTION: Possible selections of the pivot

  def chooseMedianOfMedians[T <% Ordered[T]](arr: Array[T])(implicit m: ClassTag[T]): T = {

    // below this there are corner cases
    // in many explanations of this algorithm this step is ignored
    if(arr.size <= 10) {
      val sortArr = arr.sorted
      return sortArr((arr.size - 1) / 2)
    }

    val medians = arr.grouped(5).map{case a => medianUpTo5(a)}.toArray

    if (medians.size <= 5)
      medianUpTo5 (medians)
    else {
      chooseMedianOfMedians(medians)
    }
  }


  def medianUpTo5[T <% Ordered[T]](five: Array[T]): T = {
    def order2[T <% Ordered[T]](a: Array[T], i: Int, j: Int) = {
      if (a(i)>a(j)) { val t = a(i); a(i) = a(j); a(j) = t }
    }

    def pairs[T <% Ordered[T]](a: Array[T], i: Int, j: Int, k: Int, l: Int) = {
      if (a(i)<a(k)) { order2(a,j,k); a(j) }
      else { order2(a,i,l); a(i) }
    }

    if (five.length < 2) return five(0)
    order2(five,0,1)
    if (five.length < 4) return (
      if (five.length==2 || five(2) < five(0)) five(0)
      else if (five(2) > five(1)) five(1)
      else five(2)
      )
    order2(five,2,3)
    if (five.length < 5) pairs(five,0,1,2,3)
    else if (five(0) < five(2)) { order2(five,1,4); pairs(five,1,4,2,3) }
    else { order2(five,3,4); pairs(five,0,1,3,4) }
  }

  // Random: for quickselect
  def chooseRandomPivot[T <% Ordered[T]](arr: Array[T]): T = arr(scala.util.Random.nextInt(arr.size))

  // TODO: Current implementation has ensured linear or average linear time.
  // TODO: Implement Introselect for having the best of both worlds
  // TODO: See: https://en.wikipedia.org/wiki/Introselect
}
