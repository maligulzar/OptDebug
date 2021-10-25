package examples.benchmarks.optdebug

import examples.benchmarks.AggregationFunctions
import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD
import symbolicprimitives.{SymFloat, SymInt, SymString}

/**
  * Analysis: Average Severity of the accidents on street, highway?
  *
  * */
//ID,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Number,Street,Side,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
object USAccidentAnalysisOptDebug {

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
      if(args.length ==3 ) perpartitionSize = args(2).toInt

    } //set up spark context

    val temp_path = logFile.replaceAll("/\\*", "")
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc,temp_path, perpartitionSize)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float), (SymString, SymFloat)](logFile,
                                                           fun1,
                                                           fun2,
                                                           Some(test))
  }

  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
 input
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
    ).mapValues({case (sum, count) => sum.toFloat/count})
  }

  def fun2(
      input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymString, SymFloat)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
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
    ).mapValues({case (sum, count) => sum/count.toFloat})
  }

  def test(t: (String, Float)): Boolean = {

    if (t._1.startsWith("1"))
      if (t._2 >=0.5)
        true
      else
        false
    else
      true
  }
}
