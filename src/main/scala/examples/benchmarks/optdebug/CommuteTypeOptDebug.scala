package examples.benchmarks.optdebug

import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import symbolicprimitives.{SymFloat, SymInt, SymString}
import symbolicprimitives.SymImplicits._

object CommuteTypeOptDebug {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/commute/trips/*"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float), (String, SymFloat)](logFile, fun1, fun2, Some(test))
    println("Tarantula Score" + optdebug.findFaultLocationWithSpectra())
  }


  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
    input.map { s =>
      val cols = s.split(",")
      (cols(1), cols(3).toFloat / cols(4).toFloat)
    }
      .map { s =>
        val speed = s._2
        if (speed > 300) {
          ("airplane", speed)
        }
        else if (speed > 40) {
          ("car", speed)
        }
        else if (speed > 15) {
          ("public", speed)
        }
        else if (speed > 10) {
          ("onfoot", speed)
        } else
          ("invalid", speed)
      }.reduceByKey(_ + _)
  }

  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(String, SymFloat)] = {
    input.map { s =>
      val cols = s.split(",")
      (cols(1), cols(3).toFloat / cols(4).toFloat)
    }
      .map { s =>
        val speed = s._2
        if (speed > 300) {
          ("airplane", speed)
        }
        else if (speed > 40) {
          ("car", speed)
        }
        else if (speed > 15) {
          ("public", speed)
        }
        else if (speed > 10) { //error
          ("onfoot", speed)
        } else
          ("invalid", speed)
      }.reduceByKey(_ + _)
  }

  def test(t: (String,Float)): Boolean = {
    val (str, speed) = t

    if (str == "invalid")
      if (speed <0)
        true
      else
        false
else if (speed > 0) true else false
  }
}