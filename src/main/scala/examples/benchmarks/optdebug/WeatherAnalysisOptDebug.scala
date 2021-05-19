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
import symbolicprimitives.{SymFloat, SymInt, SymString, Utils}


object WeatherAnalysisOptDebug {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float) , (SymString, SymFloat)](logFile, fun1, fun2, Some(test))
    println("Tarantula Score" + optdebug.findFaultLocationWithSpectra())
  }

  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
    val split = input.flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf('/'))
      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf('/'))
      List[(String, Float)](
        (monthdate, snow),
        (year, snow)
      ).iterator
    }
    AggregationFunctions.minMaxDeltaByKey(split)
 }
  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymString, SymFloat)] = {
    val split = input.flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf('/'))
      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf('/'))
      List[(SymString, SymFloat)](
        (monthdate, snow),
        (year, snow)
      ).iterator
    }
    AggregationFunctions.minMaxDeltaByKey(split)
  }

  def convert_to_mm(s: SymString): SymFloat = {
  val unit = s.substring(s.length - 2)
  val v = s.substring(0, s.length - 2).toFloat
  if( unit.equals("mm") ) return v else v * 304.8f
}


  def zipToState(str: SymString): SymString = {
  (str.toInt % 50).toSymString
}

  def convert_to_mm(s: String): Float = {
  val unit = s.substring(s.length - 2)
  val v = s.substring(0, s.length - 2).toFloat
  if( unit.equals("mm") ) return v else v * 304.8f
}


  def zipToState(str: String):String = {
  (str.toInt % 50).toString
}

  def test(t: (String, Float)): Boolean = t._2 < 6000f
}


