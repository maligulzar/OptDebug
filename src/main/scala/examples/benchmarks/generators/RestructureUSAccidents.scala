package examples.benchmarks.generators

import java.io.File

import examples.benchmarks.generators.AirportTransitDataGenerator.deleteDir
import org.apache.spark.{SparkConf, SparkContext}

object RestructureUSAccidents {

  //ID,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Number,Street,Side,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
 sparkConf.setMaster("local[6]")
      sparkConf
        .setAppName("Airport Transit Time Analysis")
        .set("spark.executor.memory", "2g")
      var logFile = "datasets/accidentdata/US_Accidents_Dec20_Updated.csv"
  val ctx = new SparkContext(sparkConf)

    var re_logfile = logFile.replace(".csv" , "_restruct.csv")
    if(new File(re_logfile).exists()){
      deleteDir(new File(re_logfile))
    }
    ctx.textFile(logFile).map(s => s.split(",")
    ).filter{s=>
      if(s(1) == "1")
        true
       else if(s(25) == "nan")
        false
      else true
    }.map(s => s.reduce(_+","+_)).saveAsTextFile(re_logfile)
  }


}
