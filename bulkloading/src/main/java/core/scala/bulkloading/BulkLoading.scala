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
import org.apache.hadoop.io.Writable
import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.LocatedFileStatus
import scala.util.matching.Regex
import org.apache.spark.util.LongAccumulator

/**
 *
 * Sort based bulk loading of R tree using dynamic programming scheme
 *
 * @see  D Achakeev, B Seeger and P Widmayer:
 * "Sort-based query-adaptive loading of R-trees" in CIKM 2012
 *
 *
 *
 * TODO:
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
  def bulkLoad[K <: Writable](rootDirectory: String, sc: SparkContext, rectangles: RDD[(K, RectangleTuple)], sfc: RectangleTuple => Long, numPartitions: Int, b: Int, B: Int, maxBatchSize: Int)(implicit c: ClassTag[K]): Unit = {
    var level = 0;
    val accumulator:LongAccumulator = sc.longAccumulator("levelCounter")
    rectangles.map(x => (sfc(x._2), x)).sortByKey(true, numPartitions).map(y => y._2).foreachPartition(partitionsUDF[K](accumulator,level, b: Int, B: Int, maxBatchSize, rootDirectory))
    var createdIndexEntries: Long = accumulator.value
    while (createdIndexEntries > B) {
      // Read from temporary file and create rdd, since data has been already sorted use total order partitioner 
      // compute input for range partitioner
      val numberOfPartitions = {
        //TODO
        // compute desired number of tasks/partitions one spark task >= maxBatchSize
        10
      }
      val stringLevel = "%02d".format(level)
      val MetaInfoRegEx: Regex = ("meta_" + stringLevel).r
      val metaRDD = getFiles(rootDirectory, sc, MetaInfoRegEx, classOf[PartitionKeyTuple], classOf[NullWritable]).map(pair => pair._1)
      val rangePartioner = computePartitions(numberOfPartitions, metaRDD)
      val RegEx: Regex = ("mbr_" + stringLevel).r
      val indexLevelInput : RDD[(PartitionKeyTuple, RectangleTuple)] = getFiles(rootDirectory, sc, RegEx,classOf[PartitionKeyTuple], classOf[RectangleTuple])
      val nextLevelAccumulator = sc.longAccumulator("acc_"+ "%02d".format(level))
      level += 1;
      // process next level
      indexLevelInput.partitionBy(rangePartioner).foreachPartition(partitionsUDF(nextLevelAccumulator,level, b, B, maxBatchSize, rootDirectory))
      createdIndexEntries = nextLevelAccumulator.value
    }
  }
  
  /**
   * 
   */
  def getFiles[K,V](rootDirectory: String, sc: SparkContext, regex  : Regex, k : Class[K] , v : Class[V] )   : RDD[(K,V)] = {
    val pathRootDirectory = new Path(rootDirectory)
    val fs: FileSystem = pathRootDirectory.getFileSystem(new Configuration())
    val remoteIterator = fs.listFiles(pathRootDirectory, false)
    var rdd: RDD[(K,V)] = null
    while (remoteIterator.hasNext()) {
      val status: LocatedFileStatus = remoteIterator.next()
      val path: Path = status.getPath
      val matchFileName = regex.findFirstIn(path.getName)
      if (!matchFileName.isEmpty) {
        if(rdd == null){
          rdd = sc.sequenceFile(path.getName, k, v)
        }else{
          val nextRDD = sc.sequenceFile(path.getName, k, v)
          rdd.union(nextRDD)
        }
      }
    }
    return rdd
  }

 
  /**
   * read partition information from the previous step  information
   */
  def computePartitions(numberOfPartitions: Int, rangePairs: RDD[PartitionKeyTuple]): Partitioner = {
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
   * // we use FileSystem of Hadoop fs -> done
   * // take at most maxBatchSize e.g B² elements compute partitioning -> done
   * // write new MBRINfo objects for next level -> done
   * // write in binary format -> done
   * // write infos about partitions partition number and number of prodced MBRs -> done
   *
   * //TODO
   * // current implementation of union is not efficient, since we loop over array twice
   *
   * K is a generic key for a leaf level it is a Space Filling Curve Value
   *  for a index level it is a tuple consisting of partiotion if and sequence number in this partition
   *
   */
  def partitionsUDF[K](accumulator:LongAccumulator, level: Int, b: Int, B: Int, maxBatchSize: Int, rootDirectory: String)(iter: Iterator[(K, core.scala.RectangleTuple)])(implicit c: ClassTag[K]): Unit = {
    val partid: Int = TaskContext.getPartitionId() // take information from a spark task context
    val pathRootDirectory = new Path(rootDirectory);
    val fs = pathRootDirectory.getFileSystem(new Configuration())
    val conf = new Configuration();
    conf.set("fs.defaultFS", pathRootDirectory.getName);
    val stringPartId = "%05d".format(partid)
    val stringLevel = "%02d".format(level)
    // define paths & writer
    val pathOutPutNextLevel = new Path(rootDirectory + "/mbr_" + level + "_" + stringPartId) // next level here we store pointers/MBR to level
    fs.delete(pathOutPutNextLevel, true)
    val pathOutPutNextLevelWriter = SequenceFile.createWriter(conf, Writer.file(pathOutPutNextLevel), Writer.keyClass(classOf[core.scala.BulkLoading.PartitionKeyTuple]), Writer.valueClass(classOf[core.scala.RectangleTuple]))
    val pathOutPutMeta = new Path(rootDirectory + "/meta_" + level + "_" + stringPartId) // meta information path 
    fs.delete(pathOutPutMeta, true)
    val pathOutPutMetaWriter = SequenceFile.createWriter(conf, Writer.file(pathOutPutMeta), Writer.keyClass(classOf[core.scala.BulkLoading.PartitionKeyTuple]), Writer.valueClass(classOf[NullWritable]))
    var pathOutPutSortedRecs: Path = null
    var pathOutPutSortedRecsWriter: Writer = null
    if (level == 0) {
      pathOutPutSortedRecs = new Path(rootDirectory + "/rec_" + stringPartId) // sorted input path, only for level 0 
      fs.delete(pathOutPutSortedRecs, true)
      pathOutPutSortedRecsWriter = SequenceFile.createWriter(conf, Writer.file(pathOutPutSortedRecs), Writer.keyClass(c.runtimeClass), Writer.valueClass(classOf[core.scala.RectangleTuple]))
    }
    // main loop split in batches group with a max
    val bufferedIterator = iter grouped maxBatchSize
    var rank: Int = 0;
    try {
      while (bufferedIterator.hasNext) {
        // get rectangles array
        val list = bufferedIterator.next()
        if (level == 0) {
          for (pair <- list) {
            pathOutPutSortedRecsWriter.append(pair._1, pair._2);
          }
        }
        val array = list.map(pairWithTuple => pairWithTuple._2).toArray
        // minimize area  using dynamic programming 
        val bestCostPartitioningRoot = Partitions.computePartition(array, b, B, Partitions.costFunctionArea)
        val bestCostPartitioning = Partitions.getIndexArray(bestCostPartitioningRoot)
        var index = 0
        for (i <- 0 until bestCostPartitioning.length) {
          val numberOfRectangles = bestCostPartitioning(i)
          val indexMBR = array(index).clone()
          for (j <- index + 1 until index + numberOfRectangles) {
            indexMBR.union(array(j))
          }
          index += numberOfRectangles
          pathOutPutNextLevelWriter.append(new PartitionKeyTuple(partid, rank), indexMBR)
          rank += 1
          accumulator.add(1)
        }
      }
      pathOutPutMetaWriter.append(new PartitionKeyTuple(partid, rank - 1), NullWritable.get()) // XXX can we use additionally MBR over the whole partitions :-) ???
    } finally {
      if (level == 0) {
        pathOutPutSortedRecsWriter.close();
      }
      pathOutPutNextLevelWriter.close()
      pathOutPutMetaWriter.close()
    }
  }
  /**
   * Key class for a index level and meta information
   */
  class PartitionKeyTuple(partId: Int, rank: Int) extends Serializable with Writable {

    var partitionId = partId
    var rankInPartition = rank

    override def write(out: DataOutput): Unit = {
      out.writeInt(partitionId)
      out.writeInt(rankInPartition)
    }

    override def readFields(in: DataInput): Unit = {
      partitionId = in.readInt()
      rankInPartition = in.readInt()
    }
  }

  def main(args: Array[String]): Unit = {
    val pairOne: RectangleTuple = new RectangleTuple(Array(1, 1), Array(2, 2))
    val pairTwo: RectangleTuple = new RectangleTuple(Array(3, 3), Array(4, 4))
    val p = new RectangleTuple()
    val list = List((new LongWritable(1L), pairOne))
    val path = new Path("file:/home/d/t")

  }

}