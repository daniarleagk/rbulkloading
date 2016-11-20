package core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.RangePartitioner
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object BulkLoading {

  /**
   *
   * main bulk-loading method
   */
  def bulkLoad(rootDirectory: String, sc: SparkContext, rectangles: RDD[RectangleTuple], sfc: RectangleTuple => Long, numPartitions: Int, b: Int, B: Int, maxBatchSize: Int): Unit = {
    var level = 0;
    // sort and process input rectangles process non leaf levels
    rectangles.map(x => (sfc(x), (Option.empty[MBRInfo], x))).sortByKey(true, numPartitions).foreachPartition(partitionsUDF(level, b: Int, B: Int, maxBatchSize, rootDirectory))
    while (getNumberOfGeneratedObjects(rootDirectory, level) > B) {
      //partitionsUDF
      // Read from temporary file and create rdd
      // since data has been already sorted use total order partitioner 
      // compute input for range partitioner
      // since each partitionsUDF 
      val numberOfPartitions = {
       // to compute 
        // read 
        // compute desired number of tasks/partitions one spark task >= maxBatchSize
      }
//      val rangePartioner = computePartitons( , rangePairs: RDD[(Int, Int)]) 
      level += 1;
    }
  }
  /**
   * read information about how many MBR are generated
   */
  def getNumberOfGeneratedObjects(path: String, level: Int): Int = {
    //TODO
    // hadoop fs read meta data for a given level
    0
  }
  /**
   * read partition information from the previous step  information
   */
  def computePartitons(numberOfPartitions : Int, rangePairs: RDD[(Int, Int)]): Partitioner = {
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

  /**
   * for each partition function level
   *
   * we save three files
   * 1. sorted input rectangles
   * 2. generated next level MBRs as a key value object: key:= partitionId, sequence number (rank)
   * value: number of rectangles that belongs to a MBR and MBR
   * 3. partition id and overall number of processed in this partition
   *
   * // TODO we use FileSystem of Hadoop fs
   *
   *  //TODO
   * // take at most maxBatchSize e.g B² elements compute partitioning
   * // write new MBRINfo objects for next level
   * // write in binary format
   * // write infos about partitions partition number and number of prodced MBRs
   * // this information is used to compute a
   */
  def partitionsUDF(level: Int, b: Int, B: Int, maxBatchSize: Int, rootDirectory: String)(iter: Iterator[(Long, (Option[MBRInfo], core.scala.RectangleTuple))]): Unit = {
    val partid = TaskContext.getPartitionId() // take information from a spark task context
    val pathRootDirectory = new Path(rootDirectory);
    val fs = pathRootDirectory.getFileSystem(new Configuration())
    val stringPartId = "%05d".format(partid)
    val stringLevel = "%02d".format(level)
    val pathOutPutSortedRecs = new Path(rootDirectory + "/rec_" + stringPartId) // sorted input path, only for level 0 
    val pathOutPutNextLevel = new Path(rootDirectory + "/mbr_" + level + "_" + stringPartId) // next level here we store pointers/MBR to level
    val pathOutPutMeta = new Path(rootDirectory + "/meta_" + level + "_" + stringPartId) // meta information path 
    // group with a max
    val bufferedIterator = iter grouped maxBatchSize
    while (bufferedIterator.hasNext) {
      val array = bufferedIterator.next().map(pairWithTuple => pairWithTuple._2._2).toArray
      // minimize area
      val bestCostPartitioningRoot = Partitions.computePartition(array, b, B, Partitions.costFunctionArea)
      val bestCostPartitioning = Partitions.getIndexArray(bestCostPartitioningRoot)
      // compute mbrs and write all informations
      //TODO
    }
  }

}
/**
 * single information stored after processing the first level
 */
class MBRInfo(partitionId: Int, localId: Int, size: Int) extends Tuple3[Int, Int, Int](partitionId, localId, size)