package examples.benchmarks.optdebugBaselineCoverage

import org.apache.spark.{SparkConf, SparkContext}

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
        "datasets/hdfs/accident-lineage/*"

    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("US Accident")

    } //set up spark context
 val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage

    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
    ctx.textFile(logFile).zipWithIndex().filter(_._2 % 4 == 0).map(s=>s._1)
      .map { s =>
        var arr = s.split(",")
        var dist = arr(8)
        var street = arr(10)
        var weather = arr(29)
        var vis = arr(25)
        var up_vis =
          if (vis == "nan") {
            avg_vis.getOrElse(weather, 0f)
          } else vis.toFloat
        (arr(1)+arr(13), up_vis)
      }.aggregateByKey((0.0f, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toFloat/count}).collect()
  }

}
