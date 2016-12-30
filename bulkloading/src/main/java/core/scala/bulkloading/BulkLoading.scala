package core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.RangePartitioner
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.util.Options
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Writable
import scala.reflect.ClassTag
import org.apache.hadoop.io.LongWritable

/**
 * 
 * Sort based bulk loading of R tree using dynamic programming scheme 
 *  
 * @see  D Achakeev, B Seeger and P Widmayer:
 * "Sort-based query-adaptive loading of R-trees" in CIKM 2012  
 * 
 * 
 * 
 */
object BulkLoading {

  /**
   *
   * 1. strategy process one level at time for each partition write file and write meta file that contains partition information such as number of items and partition id. 
   * this information is used for initializing the partitioner for a next level
   * 2. alternative we do not write a meta information, instead, we compute from a file names a partition id and count  the number of items  
   * 
   * main bulk-loading method
   */
  def bulkLoad[K<: Writable](rootDirectory: String, sc: SparkContext, rectangles: RDD[(K, RectangleTuple)], sfc: RectangleTuple => Long, numPartitions: Int, b: Int, B: Int, maxBatchSize: Int)(implicit c : ClassTag[K]): Unit = {
    var level = 0;
    // sort and process input rectangles process  for leaf level
    // output sorted set of rectanges and index level MBRS with additional information:  partitionId, sequence number (rank)
   // value: number of rectangles that belongs to a MBR and MBR
    rectangles.map(x => (sfc(x._2),  x)).sortByKey(true, numPartitions).map(y => y._2).foreachPartition(partitionsUDF[K](level, b: Int, B: Int, maxBatchSize, rootDirectory))
    
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
   * 
   * generic "for each" function
   * 
   * for leaf level it produces three files. 
   * 
   * for index level two files the next index level and partitioning information file 
   *  
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
   * 
   * 
   * K is a generic key for a leaf level it is a Space Filling Curve Value
   *  for a index level it is a tuple consisting of partiotion if and sequence number in this partition
   * 
   */
  def partitionsUDF[K](level: Int, b: Int, B: Int, maxBatchSize: Int, rootDirectory: String)(iter: Iterator[(K, core.scala.RectangleTuple)])(implicit c : ClassTag[K]): Unit = {
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
    
    while (bufferedIterator.hasNext){
      // get rectangles array
      val list =  bufferedIterator.next()
      if(level == 0){
        writeFile( pathOutPutSortedRecs, list.iterator)
      }
        
      val array = list.map(pairWithTuple => pairWithTuple._2).toArray
      // minimize area dynamic programming 
      val bestCostPartitioningRoot = Partitions.computePartition(array, b, B, Partitions.costFunctionArea)
      val bestCostPartitioning = Partitions.getIndexArray(bestCostPartitioningRoot)
      var index = 0
      for(i <- 0 until bestCostPartitioning.length){ 
        val numberOfRectangles = bestCostPartitioning(i)
        //XXX simple union function 
        val indexMBR  = array(index)
        for(j <- index+1 until index + numberOfRectangles ){
          indexMBR.union(array(j))
        }
        index += numberOfRectangles // XXX
        //
      }
      // compute mbrs and write all informations
      //TODO
    }
  }
  
  def writeFile[K]( path : Path, iter: Iterator[(K, core.scala.RectangleTuple)])(implicit c : ClassTag[K] ) = {
    val conf = new Configuration();
    conf.set("fs.defaultFS", path.getName);
    val writer = SequenceFile.createWriter(conf, Writer.file(path),  Writer.keyClass(c.runtimeClass), Writer.valueClass(classOf[core.scala.RectangleTuple]))
    for(pair <- iter)
      writer.append(pair._1, pair._2)
    writer.close()    
  }
  
  def main(args: Array[String]): Unit = {
    val pairOne : RectangleTuple = new RectangleTuple(Array(1,1) , Array(2,2))
    val pairTwo : RectangleTuple = new RectangleTuple(Array(3,3) , Array(4,4))
    val p  = new RectangleTuple()
    val list = List((new LongWritable(1L) ,pairOne))
    val path = new Path("file:/home/d/t")
    writeFile[LongWritable](path, list.iterator)
    
    
  }

}