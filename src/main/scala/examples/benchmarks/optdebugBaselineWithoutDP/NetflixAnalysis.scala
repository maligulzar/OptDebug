package examples.benchmarks.optdebugBaselineWithoutDP

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymInt, SymString}


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
      logFile = "datasets/netflixdata/netflix_titles_restruct/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Netflix")
    } //set up spark context
    val nsc= new SparkContext(sparkConf)
    val scdp = new SparkContextWithDP(nsc)//set up lineage context and start capture lineage
    val input = scdp.textFileSymbolic(logFile)
 input
      .map { s =>
        val row = s.split(",")
        val release = row(7).toInt
        val added =
          if (row(6).isEmpty)
            SymString("0")
          else
            row(6).trim().split("-| ")(2)
        var added_year =
        if (added.length == 2) {
           added.toInt + 2000
        } else {
          added.toInt
        }
        (added_year - release, SymInt(1))
      }
      .reduceByKey(_ + _).collect()
  }
}
