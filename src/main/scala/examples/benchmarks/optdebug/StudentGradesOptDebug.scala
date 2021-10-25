package examples.benchmarks.optdebug

import optdebug.OptDebug
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import provenance.rdd.{PairProvenanceRDD, ProvenanceRDD}
import symbolicprimitives.{SymFloat, SymInt, SymString}

/**
  * Analysis: How old was the movie when it was added?
  * */
object StudentGradesOptDebug {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var perpartitionSize  = 10000
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf
        .setAppName("Student GPA Analysis")
        .set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesV2/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Student")
      if(args.length ==3 ) perpartitionSize = args(2).toInt
    } //set up spark context

    val temp_path = logFile.replaceAll("/\\*", "")
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val lc = new LineageContext(ctx)
    val optdebug = new OptDebug(lc, temp_path, perpartitionSize)
    lc.setCaptureLineage(true)
    optdebug.runWithOptDebug[(String, Float), (SymString, SymFloat)](logFile,
                                                                 fun1,
                                                                 fun2,
                                                                 Some(test))
  }

  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
    input
    input.map(line => {
      val arr = line.split(",")
      val (studentID, grade) = (arr(0), arr(2).toInt)

      (studentID, grade)
    }).mapValues(grade => {
      if (grade >= 93)
        4.0f
      else if (grade >= 90)
        3.7f
      else if (grade >= 87)
        3.3f
      else if (grade >= 83)
        3.0f
      else if (grade >= 80)
        2.7f
      else if (grade >= 77)
        23f
      else if (grade >= 73)
        2.0f
      else if (grade >= 70)
        1.7f
      else if (grade >= 67)
        1.3f
      else if (grade >= 65)
        1.0f
      else 0.0f
    }).aggregateByKey((0.0f, 0f))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum/count})
  }

  def fun2(input: ProvenanceRDD[SymString]): ProvenanceRDD[(SymString, SymFloat)] = {
    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
   input.map(line => {
      val arr = line.split(",")
      val (studentID, grade) = (arr(0), arr(2).toInt)

      (studentID, grade)
    }).mapValues(grade => {
      if (grade >= 93)
        SymFloat(4.0f, grade.getProvenance())
      else if (grade >= 90)
        SymFloat(3.7f, grade.getProvenance())
      else if (grade >= 87)
        SymFloat(3.3f, grade.getProvenance())
      else if (grade >= 83)
        SymFloat(3.0f, grade.getProvenance())
      else if (grade >= 80)
        SymFloat(2.7f, grade.getProvenance())
      else if (grade >= 77)
        SymFloat(23f, grade.getProvenance())
      else if (grade >= 73)
        SymFloat(2.0f, grade.getProvenance())
      else if (grade >= 70)
        SymFloat(1.7f, grade.getProvenance())
      else if (grade >= 67)
        SymFloat(1.3f, grade.getProvenance())
      else if (grade >= 65)
        SymFloat(1.0f, grade.getProvenance())
      else
        SymFloat(0.0f, grade.getProvenance())
    }).aggregateByKey((SymFloat(0.0f), SymFloat(0f)))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum/count})
  }

  def test(t: (String, Float)): Boolean = t._2 >= 0 && t._2 <= 4.0
}
