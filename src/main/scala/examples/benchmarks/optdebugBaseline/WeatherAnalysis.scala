package examples.benchmarks.optdebugBaseline

import org.apache.spark.{SparkConf, SparkContext}



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

    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage


  val input  = ctx.textFile(logFile)
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
        (monthdate + state, snow),
        (year + state, snow)
      ).iterator
    }


    split.aggregateByKey[(Float, Float)]((Float.MaxValue, Float.MinValue))(
      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
      // If we didn't define udfAware flag globally, we could override enableUDFAwareProv here
      .mapValues({ case (min, max) => max - min }).collect()
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
}


