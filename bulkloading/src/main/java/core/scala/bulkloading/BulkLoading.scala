package core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.RangePartitioner
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

object BulkLoading {

  /**
   *
   * main bulk-loading method
   */
  def bulkLoad(sc: SparkContext, rectangles: RDD[RectangleTuple], sfc: RectangleTuple => (Long, RectangleTuple), numPartitions: Int, b: Int, B: Int): Unit = {
    val levelOutputAccumulator = sc.accumulator(0, "OutputMBRs");
    /**
     * foreach partition function leaf level
     */
    def partitionsLeafUDF(iter: Iterator[(Long, core.scala.RectangleTuple)]): Unit = {
      //TODO
      // take at most B² elements compute partitioning 
      // write new MBRINfo objects for next level 
      // update accumulator 
      // write in binary format
      // write infos about partitions partition number and size as last operation
      // this information is used to compute a
      val partid = TaskContext.getPartitionId()
      levelOutputAccumulator.add(1)
    }
    //
    /**
     * foreach partition function 
     */
    def partitionsUDF(iter: Iterator[(Long, core.scala.MBRInfo, core.scala.RectangleTuple)]): Unit = {
      //TODO
      // take at most B² elements compute partitioning 
      // write new MBRINfo objects 
      // 
      val partid = TaskContext.getPartitionId()
    }
    // sort and process input rectangles
    rectangles.map(sfc).sortByKey(true, numPartitions).foreachPartition(partitionsLeafUDF)
    // process non leaf levels
  
    //main loop
    while (levelOutputAccumulator.value > B) {
      levelOutputAccumulator.setValue(0)
      //partitionsUDF
      // Read from temporary file and create rdd
      // since data has been already sorted use total order partitioner 
      // compute input for range partitioner
      // since each partitionsUDF 
//      val rangePartioner = computePartitons(rangePairs: RDD[(Int, Int)])
    }
  }
 /**
  * read partition information from the previous step  information 
  */
 def computePartitons(rangePairs: RDD[(Int, Int)]) : Partitioner ={
   val numberOfPartitions = 0
   val part = new Partitioner {
        
     override def numPartitions: Int = {
       numberOfPartitions
     }
     
     override def getPartition(key: Any): Int = {
       //TODO binary tree
       0
     }
   }
  
   part
 } 
  
}
/**
 * single information stored after processing the first level
 */
class MBRInfo(partitionId: Int, localId: Int, size: Int) extends Tuple3[Int, Int, Int](partitionId, localId, size)