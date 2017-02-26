package core.scala

import org.apache.hadoop.io.Writable
import java.io.DataOutput
import java.io.DataInput

object Partitions {

  /**
   * gopt partitioning
   * gopt(i) := min{𝑏≤𝑗≤𝐵}{gopt(i-j) + Area(MBR(𝑝𝑖−𝑗+1,𝑖))} 
   * 
   *  
   * 
   * XXX: different optimizations, store mbrs in DP costs array 
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
   * reverse traversal for a best cost partitioning
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
  
  /**
   * area cost function
   */
  def costFunctionArea(mbr : RectangleTuple) : Double = {
    (mbr.left zip mbr.right).map(t => (t._2 - t._1).abs).reduceLeft(_*_)
  }

}
/**
 * simple pojo class to store costs for DP
 * 
 * //TODO can be extended with a MBR information
 */
class Bucket(val cost: Double, val start: Int, val end: Int, var size: Int = 0, val pred: Bucket = null)
/**
 * simple pojo class to store MBRs
 */
class RectangleTuple(l: Array[Double], r: Array[Double]) extends Serializable with Writable with Cloneable{
  require(l.size == r.size, "left and right point should have same dimension")
  var left = l
  var right = r
  lazy val  dimension = left.size
  /**
   * empty constructor
   */
  def this() = this(Array(), Array())
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
  /**
   * 
   */
  def getCenter(): Array[Double] = {
    val center = new Array[Double](left.size)
    for ( i <- 0 until dimension){
      center(i) = (right(i) - left(i))/2
    }
    center
  }
  /**
   * 
   */
  def getSideLength(): Array[Double] = {
    val sideLength = new Array[Double](left.size)
    for(i <- 0 until dimension){
      sideLength(i) = right(i) - left(i)
    }
    sideLength
  }
  /**
   * 
   */
  override def write(out : DataOutput): Unit={
    out.writeInt(left.length)
    for(i <- 0 until left.length){
      out.writeDouble(left(i))
    }
    for(i <- 0 until right.length){
      out.writeDouble(right(i))
    }
  }
  /**
   * 
   */
  override def readFields(in : DataInput): Unit={
    val dimension = in.readInt()
    println(dimension)
    val leftP =  new  Array[Double](dimension)
    val rightP = new Array[Double](dimension)
    for (i <- 0 until dimension){
      leftP(i) = in.readDouble()
    }
    for (i <- 0 until dimension){
      rightP(i) = in.readDouble()
    }
    left = leftP
    right = rightP
  }
  
  
  override def clone(): RectangleTuple ={
    val leftArray = new Array[Double](left.size)
    val rightArray = new Array[Double](right.size)
    Array.copy(left,0, leftArray, 0, leftArray.size)
    Array.copy(right,0, rightArray, 0, rightArray.size)
    new RectangleTuple(leftArray, rightArray)
  }
  

}
