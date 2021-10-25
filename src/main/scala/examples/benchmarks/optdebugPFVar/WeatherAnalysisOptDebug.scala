package examples.benchmarks.optdebugPFVar

import examples.benchmarks.AggregationFunctions
import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import symbolicprimitives.{SymFloat, SymString}


object WeatherAnalysisOptDebug {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var perpartitionSize  = 10000
    var logFile = ""
    var (p,f) =(2,2);
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Weather")
      if(args.length ==3 ) perpartitionSize = args(2).toInt
      if(args.length ==4 ) p = args(3).toInt
      if(args.length ==5 ) f = args(4).toInt

    } //set up spark context

    val temp_path = logFile.replaceAll("/\\*", "")

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc, temp_path, perpartitionSize ,SELECTIVITY_FACTOR_SUCCESS = p,SELECTIVITY_FACTOR_FAILURE = f)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float) , (SymString, SymFloat)](logFile, fun1, fun2, Some(test))
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
        (monthdate + state , snow),
        (year + state, snow)
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
        (monthdate + state, snow),
        (year + state , snow)
      ).iterator
    }
    AggregationFunctions.minMaxDeltaByKey(split)
  }

  def convert_to_mm(s: SymString): SymFloat = {
  val unit = s.substring(s.length - 2)
  val v = s.substring(0, s.length - 2).toFloat
  if( unit.equals("mm") )
     v
  else
    v * 304.8f
}


  def zipToState(str: SymString): SymString = {
  (str.toInt % 50).toSymString
}

  def convert_to_mm(s: String): Float = {
  val unit = s.substring(s.length - 2)
  val v = s.substring(0, s.length - 2).toFloat
  if( unit.equals("mm") )
     v
  else
    v * 304.8f
}


  def zipToState(str: String):String = {
  (str.toInt % 50).toString
}

  def test(t: (String, Float)): Boolean = t._2 < 6000f
}


