package examples.benchmarks.optdebugBaselineWithoutDP

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP


object CommuteType {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/commute/trips/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Commute")
    } //set up spark context
    val nsc= new SparkContext(sparkConf)
    val scdp = new SparkContextWithDP(nsc)//set up lineage context and start capture lineage
    val input = scdp.textFileSymbolic(logFile)
      .map { s =>
        val cols = s.split(",")
        (cols(1), cols(3).toFloat / cols(4).toFloat)
      }
      .map { s =>
        val speed = s._2
        if (speed > 70) {
          ("airplane" + s._1, speed)
        }
        else if (speed > 40) {
          ("car" + s._1, speed)
        }
        else if (speed > 15) {
          ("public" + s._1, speed)
        }
        else if (speed > 10) {
          ("onfoot" + s._1, speed)
        } else
          ("invalid" + s._1, speed)
      }.reduceByKey(_ + _).collect()
  }
}