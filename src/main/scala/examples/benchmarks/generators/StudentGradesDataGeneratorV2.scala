package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Adapted from StudentGradesDataGenerator
  * New Schema consists of: StudentID,CourseID,Grade
  */
object StudentGradesDataGeneratorV2 {
  
  var logFile = ""
  var partitions = 10
  var dataper  = 500000 // 500000 before
  val depts = Seq("EE", "CS", "MATH", "Physics ", "STATS")
  val courseNums = Seq(0,100).flatMap(x => (1 to 99).map(_ + x))
  val faultTargetCourses = Seq("CS9", "CS11")//, "CS14", "CS17") // (note: there are 5 dept x
  val faultTargetStudents = Seq("SS1200", "SS8199")
  // 80 and 50M rows, so 2 courses should equal 250K records.
  // course numbers each for a total of 200, so this represents a small fraction of total records)
  def shouldInjectFault(course: String): Boolean = faultTargetCourses.contains(course)// && Random.nextDouble() <= faultRate
  
  
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()
    val random = new Random(42) // fixed seed for reproducability
    
    
    
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("StudentGradesGenerator").set("spark.executor.memory", "2g")
      logFile =  "datasets/studentGradesV2"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val grades = logFile
    FileUtils.deleteQuietly(new File(grades))
    
    
    
    val courses = depts.flatMap(dept => courseNums.map(dept + _))
    
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>
  
        val studentId = "SS"+ (random.nextInt(9000) + 1000)
        val course = courses(random.nextInt(courses.length))
        // new strat: if course is fault, give subset a different grade
          var grade = random.nextInt(35) + 65
          if(77 <= grade && grade < 80){
          if(!faultTargetStudents.contains(studentId)){
            grade  = 76
          }else{
            println("injecting fault")
          }
          }
        val str = s"$studentId,$course,$grade"
        str
      }.toIterator
    }.saveAsTextFile(grades)
    
    println(s"Wrote file to $grades")
  }
}

/**
 *

  import scala.util.Random
var logFile = "hdfs://zion-headnode:9000/student/socc21/grades/"
  var partitions = 1000
  var dataper  = 5000000
  val depts = Seq("EE", "CS", "MATH", "Physics ", "STATS")
  val courseNums = Seq(0,100).flatMap(x => (1 to 99).map(_ + x))
  val faultTargetCourses = Seq("CS9", "CS11")//, "CS14", "CS17") // (note: there are 5 dept x
  val faultTargetStudents = Seq("SS12000", "SS81999")
val random = new Random(42) // fixed seed for reproducability
 val courses = depts.flatMap(dept => courseNums.map(dept + _))


sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>

        val studentId = "SS"+ (random.nextInt(90000) + 10000)
        val course = courses(random.nextInt(courses.length))
        // new strat: if course is fault, give subset a different grade
          var grade = random.nextInt(35) + 65
          if(77 <= grade && grade < 80){
          if(!faultTargetStudents.contains(studentId)){
            grade  = 76
          }
          }
        val str = s"$studentId,$course,$grade"
        str
      }.toIterator
    }.saveAsTextFile(logFile)

 */
