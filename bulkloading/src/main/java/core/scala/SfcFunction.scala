package core.scala

import java.util.Arrays

/**
 * provides simple space filling curves methods
 */
object SfcFunction {
  
  val LongZKeyBits:Int = 63;
  /**
   * 
   * 1. get center point
   * 2. normalize to unit cube
   * 3. map to integer array
   * 4. compute z-order key
   * @return  symmetric z-order value as long
   */
  def getSymmetricCyclicZOrderKeyFunction(universe: RectangleTuple, universeSideLength : Array[Double])= (r : RectangleTuple ) => {
    val intPoint = normalize(universe, universeSideLength, r)
    computeSymmetricCyclicZOrderKey(intPoint)
  }
  /**
   * if rectangle is already normalized to unit cube. 
   * 1. get center point
   * 2. map to integer array
   * 3. compute z-order key
   * @return  symmetric z-order value as long
   */
  def getSymmetricCyclicZOrderKeyFunctionNormalizedInput() = (r : RectangleTuple) => {
    val center : Array[Double] = r.getCenter()
    computeSymmetricCyclicZOrderKey ( mapToIntValues (center)) 
  }
  
  /**
   * normalize to unit cube and map to integer value
   * 
   * since we have 63 bit for a key, we use 63 div dimension as resolution for an integer mapping
   * 
   * @return normalized point coordinates
   */
  def normalize(universe: RectangleTuple, universeSideLength : Array[Double], r : RectangleTuple): Array[Int] = {
    val leftLowerUniversePoint = universe.left
    val center : Array[Double] = r.getCenter()
    // shift and normalize to unit cube
    for(i <- 0 until center.length){
      center(i) = (center(i) - leftLowerUniversePoint(i))/universeSideLength(i)   
    }
    // multiply with 1<<31
    val shift : Int = (LongZKeyBits / r.dimension) -1 
    val factor : Int = 1 << shift
    center.map ( d => (d * (factor)).intValue())
  }
  
  /**
   * @param point represented as array of double values
   * @return array of integers 
   */
  def mapToIntValues( normalizedPoint : Array[Double]): Array[Int] = { 
    val shift : Int = (LongZKeyBits / normalizedPoint.length) -1 
    val factor : Int = 1 << shift
    normalizedPoint.map ( d => (d * (factor)).intValue())
  }
  
  /**
   * simple symmetric z-order function. 
   * (non optimized) 
   * @return symmetric z-order value as long
   */
  def computeSymmetricCyclicZOrderKey(point: Array[Int]): Long = {
    val bitPerDim : Int = LongZKeyBits / point.length
    var sfcKey: Long = 0L
    var mask : Long = 1L
    val dimension : Int = point.length
    val dimMasks : Array[Int] = new Array[Int](point.length)
    Arrays.fill(dimMasks, 1 << (bitPerDim -1 ))
    var k = dimension * bitPerDim
    for ( i <- 0 until (dimension * bitPerDim) ){
      var pointI = point(i % dimension) // cyclic symmetric
      pointI = pointI & dimMasks(i % dimension)
      dimMasks(i % dimension) = dimMasks(i % dimension) >> 1
      if(pointI != 0){
        mask = 1 << (k-1)
        sfcKey |=  mask
      }
      k -= 1
    }
    sfcKey
  }
}