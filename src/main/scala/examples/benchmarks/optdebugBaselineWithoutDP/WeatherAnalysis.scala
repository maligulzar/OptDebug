package examples.benchmarks.optdebugBaselineWithoutDP

import examples.benchmarks.AggregationFunctions
import examples.benchmarks.optdebug.WeatherAnalysisOptDebug.{convert_to_mm, zipToState}
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymFloat, SymString}



object WeatherAnalysis {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var perpartitionSize  = 10000
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Weather")
    } //set up spark context

    val nsc= new SparkContext(sparkConf)
    val scdp = new SparkContextWithDP(nsc)//set up lineage context and start capture lineage
    val input = scdp.textFileSymbolic(logFile)
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
    AggregationFunctions.minMaxDeltaByKey(split).collect()

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
}


