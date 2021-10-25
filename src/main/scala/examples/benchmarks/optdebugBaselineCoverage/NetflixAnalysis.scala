package examples.benchmarks.optdebugBaselineCoverage

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Analysis: How old was the movie when it was added?
  * */
object NetflixAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var perpartitionSize  = 10000
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf
        .setAppName("Airport Transit Time Analysis")
        .set("spark.executor.memory", "2g")
      logFile = "datasets/hdfs/netflix-lineage/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Netflix")
    } //set up spark context
 val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
 val input = ctx.textFile(logFile).zipWithIndex().filter(_._2 % 4 == 0).map(s=>s._1)
 input
      .map { s =>
        val row = s.split(",")
        val release = row(7).toInt
        val added =
          if (row(6).isEmpty)
            "0"
          else
            row(6).trim().split("-| ")(2)
        var added_year = 0
        if (added.length == 2) {
          added_year = 2000 + added.toInt
        } else {
          added_year = added.toInt
        }
        (added_year - release, 1)
      }
      .reduceByKey(_ + _).collect()
  }
}
