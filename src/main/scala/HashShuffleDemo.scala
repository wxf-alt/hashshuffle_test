import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HashShuffleDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HashShuffleTest").setMaster("local[3]")
    // 1.5 版本 默认用SortShuffle
//    // 使用 hashShuffle
//    conf.set("spark.shuffle.manager", "hash")
//    // 开启 consolidate
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc: SparkContext = new SparkContext(conf)
    //    sc.setLogLevel("DEBUG")
    val outPath: String = "/tmp/spark/output/wordcount"
    val hadoopConf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(hadoopConf)
    val path: Path = new Path(outPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    val line: RDD[String] = sc.textFile("""E:\A_data\4.测试数据\spark-core数据\agent.log""", 5)
    println(line.partitions.length)

    val sort: RDD[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey((a, b) => a + b, 4)
    sort.saveAsTextFile(outPath)
    //打印rdd的debug信息可以方便的查看rdd的依赖，从而可以看到那一步产生shuffle
    println(sort.toDebugString)
  }
}
