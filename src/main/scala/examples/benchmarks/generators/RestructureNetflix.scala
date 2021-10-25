package examples.benchmarks.generators

import java.io.File

import examples.benchmarks.generators.AirportTransitDataGenerator.deleteDir
import org.apache.spark.{SparkConf, SparkContext}

object RestructureNetflix {

  val replication = 50
  //ID,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Number,Street,Side,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
 sparkConf.setMaster("local[6]")
      sparkConf
        .setAppName("Airport Transit Time Analysis")
        .set("spark.executor.memory", "2g")
      var logFile = "datasets/netflixdata/netflix_titles.csv"
  val ctx = new SparkContext(sparkConf)

    var re_logfile = logFile.replace(".csv" , "_restruct")
    if(new File(re_logfile).exists()){
      deleteDir(new File(re_logfile))
    }
    ctx.textFile(logFile).flatMap(s => (1 to replication).map(v => s)).repartition(35).saveAsTextFile(re_logfile)
  }


}

/**
  val replication = 10000
  val csv = "hdfs://zion-headnode:9000/student/socc21/netflix_titles.csv"
  val logfile = "hdfs://zion-headnode:9000/student/socc21/netflix/"
  sc.textFile(csv, 200).flatMap(s => (1 to replication).map(v => s)).saveAsTextFile(logfile)


 */

