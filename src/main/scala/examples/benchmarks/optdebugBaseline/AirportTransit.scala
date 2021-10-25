package examples.benchmarks.optdebugBaseline
import org.apache.spark.{SparkConf, SparkContext}

object AirportTransit {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    var perpartitionSize = 10000
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/airportdata"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Airport Transit Time Analysis")
  } //set up spark context

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val input = ctx.textFile(logFile)
    val map = input.map { s =>
      val tokens = s.split(",")
      val dept_hr = tokens(2).split(":")(0)
      val diff = getDiff(tokens(4), tokens(2))
      val airport = tokens(1)
      (airport + dept_hr, diff)
    }
    val fil = map.filter { v =>
      v._2 < 4
    }
    val out = fil.reduceByKey(_ + _)
    out.collect()
  }

  def getDiff(arr: String, dep: String): Float = {
    val arr_min = arr.split(":")(0).toFloat + arr.split(":")(1).toFloat/60
    val dep_min = dep.split(":")(0).toFloat + dep.split(":")(1).toFloat/60
    if(arr_min - dep_min  < 0){
      return arr_min - dep_min  - 24
    }
    return arr_min - dep_min
  }

}
