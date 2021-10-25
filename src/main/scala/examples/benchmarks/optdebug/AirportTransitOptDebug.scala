package examples.benchmarks.optdebug

import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymFloat, SymInt, SymString, Utils}

object AirportTransitOptDebug {

  def main(args: Array[String]): Unit = {
    //set up spark configuration

    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    var perpartitionSize  = 10000
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/airportdata"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Airport Transit Time Analysis")
      if(args.length ==3 ) perpartitionSize = args(2).toInt
    } //set up spark context

    val temp_path = logFile.replaceAll("/\\*", "")

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc, temp_path, perpartitionSize)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float), (SymString, SymFloat)](logFile, fun1, fun2, Some(test))
   // println("Tarantula Score" + optdebug.findFaultLocationWithSpectra())
  }

  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
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
    // TODO: define some sort of influence function
    val out = fil.reduceByKey(_+_)
      out
  }
  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymString, SymFloat)] = {
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

    val out = fil.reduceByKey(_+_)
    out
  }

  def getDiff(arr: String, dep: String): Float = {
    val arr_min = arr.split(":")(0).toFloat + arr.split(":")(1).toFloat/60
    val dep_min = dep.split(":")(0).toFloat + dep.split(":")(1).toFloat/60
    if(arr_min - dep_min  < 0){
      return arr_min - dep_min  - 24
    }
    return arr_min - dep_min
  }

  def getDiff(arr: SymString, dep: SymString): SymFloat = {
    val arr_min = arr.split(":")(0).toFloat + arr.split(":")(1).toFloat/60
    val dep_min = dep.split(":")(0).toFloat + dep.split(":")(1).toFloat/60
    if(arr_min - dep_min  < 0){
      return arr_min - dep_min  - 24
    }
    return arr_min - dep_min
  }

  def test(t: (String, Float)): Boolean = t._2 > 0


}
