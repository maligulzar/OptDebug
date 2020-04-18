package examples.benchmarks



import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP

/**
  * Created by Michael on 4/14/16.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/StudentInfo.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object StudentInfo {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var logFile = ""
    if(args.isEmpty){
      sparkConf.setAppName("Student_Info")
               .set("spark.executor.memory", "2g").setMaster("local[6]")
      logFile = "student.txt"
      // https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/datageneration/student.txt
    }else{
      logFile = args(0)
    }
    //set up spark context
    val ctx = new SparkContext(sparkConf)
    
    //set up lineage context
    val scdp = new SparkContextWithDP(ctx)
    
    
    val records = scdp.textFileProv(logFile)
    
    val grade_age_pair = records.map(line => {
      val list = line.split(" ")
      (list(4).toInt, list(3).toInt)
    })
    val average_age_by_grade = grade_age_pair.groupByKey
                                             .map(pair => {
                                               val itr = pair._2.toIterator
                                               var moving_average = 0.0
                                               var num = 1
                                               while (itr.hasNext) {
                                                 moving_average = moving_average + (itr.next() - moving_average) / num
                                                 num = num + 1
                                               }
                                               (pair._1, moving_average)
                                             })
    
    //val out = average_age_by_grade.collect()
    //out.foreach(println)
    val out = average_age_by_grade.collectWithProvenance()
    println("((Grade, Age), Provenance)")
    out.foreach(println)
    
    // REMOVED: print out the result for debugging purpose
    
    // REMOVED: getLineage and tracing
    
    println("Job's DONE!")
    ctx.stop()
    
  }
  
}