package examples.benchmarks.optdebug

import examples.benchmarks.AggregationFunctions
import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext

import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymInt, SymString, Utils}

object AirportTransitOptDebug {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/airportdata"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Int), (SymString, SymInt)](logFile, fun1, fun2, Some(test))
    println("Tarantula Score" + optdebug.findFaultLocationWithSpectra())
  }

  def fun1(input: Lineage[String]): RDD[(String, Int)] = {
    val map = input.map { s =>
      val tokens = s.split(",")
      val dept_hr = tokens(3).split(":")(0)
      val diff = getDiff(tokens(2), tokens(3))
      val airport = tokens(4)
      (airport + dept_hr, diff)
    }
    val fil = map.filter { v =>
      v._2 < 45
    }
    // TODO: define some sort of influence function
    val out = fil.reduceByKey(_+_)
      out
  }
  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymString, SymInt)] = {
    val map = input.map { s =>
      val tokens = s.split(",")
      val dept_hr = tokens(3).split(":")(0)
      val diff = getDiff(tokens(2), tokens(3))
      val airport = tokens(4)
      (airport + dept_hr, diff)
    }
    val fil = map.filter { v =>
      v._2 < 45
    }
    // TODO: define some sort of influence function
    val out = AggregationFunctions.sumByKey(fil)
    out
  }

  def getDiff(arr: String, dep: String): Int = {
    val arr_min = arr.split(":")(0).toInt * 60 + arr.split(":")(1).toInt
    val dep_min = dep.split(":")(0).toInt * 60 + dep.split(":")(1).toInt
    if(dep_min - arr_min < 0){
      return dep_min - arr_min + 24*60
    }
    return dep_min - arr_min
  }

  def getDiff(arr: SymString, dep: SymString): SymInt = {
    val arr_min = arr.split(":")(0).toInt * 60 + arr.split(":")(1).toInt
    val dep_min = dep.split(":")(0).toInt * 60 + dep.split(":")(1).toInt
    if(dep_min - arr_min < 0){
      return dep_min - arr_min + 24*60
    }
    return dep_min - arr_min
  }

  def test(t: (String, Int)): Boolean = t._2 > 0


}
