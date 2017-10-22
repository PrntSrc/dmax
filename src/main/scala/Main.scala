import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Yue on 2017/10/22.
  */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile(args(1))
    sc.stop()
  }
}
