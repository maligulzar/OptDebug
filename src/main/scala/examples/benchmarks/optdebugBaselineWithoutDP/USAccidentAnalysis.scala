package examples.benchmarks.optdebugBaselineWithoutDP

import examples.benchmarks.optdebug.USAccidentAnalysisOptDebug.avg_vis
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymFloat, SymInt}

/**
  * Analysis: Average Severity of the accidents on street, highway?
  *
  * */
//ID,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Number,Street,Side,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
object USAccidentAnalysis
{

  var avg_vis = Map[String, Float]()

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    var logFile = ""
    var perpartitionSize  = 10000
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf
        .setAppName("Airport Transit Time Analysis")
        .set("spark.executor.memory", "4g")
      logFile =
        "datasets/accidentdata/US_Accidents_Dec20_Updated_restruct.csv/*"

    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("US Accident")

    } //set up spark context
    val nsc= new SparkContext(sparkConf)
    val scdp = new SparkContextWithDP(nsc)//set up lineage context and start capture lineage
    val input = scdp.textFileSymbolic(logFile)
    input
      .map { s =>
        var arr = s.split(",")
        var dist = arr(8)
        var street = arr(10)
        var weather = arr(29)
        var vis = arr(25)
        var up_vis =
          if (vis == "nan") {
            SymFloat(avg_vis.getOrElse(weather.value, 0f),
              weather.getProvenance())
          } else
            vis.toFloat
        (arr(1)+arr(13), up_vis)
      }.aggregateByKey((SymFloat(0.0f), SymInt(0)))(
      {case ((sum, count), next) => (next + sum, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum/count.toFloat}).collect()
  }

}
