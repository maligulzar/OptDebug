package examples.benchmarks.optdebugBaselineCoverage

import org.apache.spark.{SparkConf, SparkContext}


object CommuteType {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/hdfs/trips-lineage/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Commute")
    } //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.textFile(logFile).zipWithIndex().filter(_._2 % 4 == 0).map(s=>s._1)
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