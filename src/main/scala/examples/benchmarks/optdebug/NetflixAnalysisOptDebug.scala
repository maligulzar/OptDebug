package examples.benchmarks.optdebug

import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import symbolicprimitives.{SymInt, SymString}
import symbolicprimitives.SymImplicits._

/**
  * Analysis: How old was the movie when it was added?
  * */
object NetflixAnalysisOptDebug {

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
      if(args.length ==3 ) perpartitionSize = args(2).toInt
    } //set up spark context

    val temp_path = logFile.replaceAll("/\\*", "")
   val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc, temp_path, perpartitionSize)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(Int, Int), (SymInt, SymInt)](logFile,
                                                                 fun1,
                                                                 fun2,
                                                                 Some(test))
  }

  def fun1(input: Lineage[String]): RDD[(Int, Int)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
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
      .reduceByKey(_ + _)
  }

  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymInt, SymInt)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
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
          if (added.length == 2)
            added.toInt + 2000
          else
            added.toInt

        (added_year - release, SymInt(1))
      }
      .reduceByKey(_ + _)
  }

  def test(t: (Int, Int)): Boolean = t._1 >= 0



}
