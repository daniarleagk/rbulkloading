package core.scala

object Partitions {

  /**
   *
   */
  def computePartition(rectangles: Array[RectangleTuple], b: Int, B: Int, costFunction: RectangleTuple => Double): Bucket = {
    val buckets: Array[Bucket] = new Array(rectangles.length)
    val dummyBucket: Bucket = new Bucket(Double.MaxValue, 0, 0)
    var startIndex: Int = 0
    // init first b buckets
    for (index <- 0 until b) {
      buckets(startIndex) = dummyBucket
    }
    for (t <- startIndex until rectangles.length) {
      // compute costs in path O(B) / loop back and compute a union of rectangles from start index the smallest size b to largest size B
      val costs: Array[Double] = {
        val costs = Array[Double](B) // TODO we also can use map
        val mbrUnionCurrent = new RectangleTuple(rectangles(startIndex).left.clone(), rectangles(startIndex).right.clone())
        var i = 0
        var j = 0
        for (i <- startIndex - 1 to 0 by -1) {
          mbrUnionCurrent.union(rectangles(i))
          if (j >= b - 1) {
            costs(j) = costFunction(rectangles(i))
          }
          j += 1
        }
        costs
      }
      var mincost =  if (buckets(t) != null) buckets(t).cost else dummyBucket.cost
      var st = 0
      var et = 0;
      var argMin: Bucket = null // look back for a better costs
      var j = 0
      for (j <- b - 1 until B) {
        if (t - j == 0) {
          val startBucket: Bucket = new Bucket(costs(j), 0, t)
          startBucket.size += 1
          buckets(t) = startBucket
        } else if (t - j > 0) {
          val candidate: Bucket = buckets(t - j - 1)
          val costOfBucket: Double = costs(j);
          val costOfExtension: Double = candidate.cost + costOfBucket;
          if (costOfExtension < mincost) {
            argMin = candidate
            mincost = costOfExtension
            st = t - j
            et = t
            buckets(t) = new Bucket(mincost, st, et, argMin.size + 1, argMin)
          }
        }
      }
    }
    buckets(buckets.length - 1)
  }


  /**
   *
   */
  def getIndexArray(headBucket: Bucket): Array[Int] = {
    val  array: Array[Int] = new Array(headBucket.size)
		var next: Bucket = headBucket
		for( i <- (array.length -1) to 0 by -1){
			array(i) = next.end  - next.start  + 1
			next = next.pred
		}
		array
  }

}
/**
 *
 */
class Bucket(val cost: Double, val start: Int, val end: Int, var size: Int = 0, val pred: Bucket = null)
/**
 *
 */
class RectangleTuple(l: Array[Double], r: Array[Double]) extends Serializable {
  require(l.size == r.size, "left and right point should have same dimension")
  def left = l
  def right = r
  val dimension = l.size
  /*
   * 
   */
  override def toString: String = {
    "LeftPoint: " + this.left.mkString(", ") + " \n RightPoint: " + this.right.mkString(", ")
  }
  /**
   *
   */
  def union(that: RectangleTuple): RectangleTuple = {
    require(left.size == that.left.size && right.size == that.right.size, "Rectangles should have same dimensions")
    var d: Int = 0
    for (d <- 0 until dimension) {
      left(d) = left(d) min that.left(d)
      right(d) = right(d) max that.right(d)
    }
    this
  }

}
