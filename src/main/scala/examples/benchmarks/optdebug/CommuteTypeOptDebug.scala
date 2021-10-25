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
    var perpartitionSize  = 10000
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/commute/trips/*"
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
    val optdebug = new OptDebug(lc,temp_path, perpartitionSize)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float), (String, SymFloat)](logFile, fun1, fun2, Some(test))
  }


  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
    input.map { s =>
      val cols = s.split(",")
      (cols(1), cols(3).toFloat / cols(4).toFloat)
    }
      .map { s =>
        val speed = s._2
        if (speed > 70) {
          ("airplane"+s._1, speed)
        }
        else if (speed > 40) {
          ("car"+s._1, speed)
        }
        else if (speed > 15) {
          ("public"+s._1, speed)
        }
        else if (speed > 10) {
          ("onfoot"+s._1, speed)
        } else
          ("invalid"+s._1, speed)
      }.reduceByKey(_ + _)
  }

  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(String, SymFloat)] = {
    input.map { s =>
      val cols = s.split(",")
      (cols(2), cols(3).toFloat / cols(4).toFloat)
    }
      .map { s =>
        val speed = s._2
        if (speed > 70) {
          ("airplane"+s._1, speed)
        }
        else if (speed > 40) {
          ("car"+s._1, speed)
        }
        else if (speed > 15) {
          ("public"+s._1, speed)
        }
        else if (speed > 10) { //error
          ("onfoot"+s._1, speed)
        } else
          ("invalid"+s._1, speed)
      }.reduceByKey(_ + _)
  }

  def test(t: (String,Float)): Boolean = {
    val (str, speed) = t

    if (str.startsWith("invalid"))
      if (speed <0)
        true
      else
        false
else if (speed > 0) true else false
  }
}