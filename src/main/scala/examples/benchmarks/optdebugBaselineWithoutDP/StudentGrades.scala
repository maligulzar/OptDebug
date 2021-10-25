package examples.benchmarks.optdebugBaselineWithoutDP

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.SymFloat


/**
  * Analysis: How old was the movie when it was added?
  * */
object StudentGrades
{

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
    } //set up spark context

    val nsc= new SparkContext(sparkConf)
    val scdp = new SparkContextWithDP(nsc)//set up lineage context and start capture lineage
    val input = scdp.textFileSymbolic(logFile)

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
    ).mapValues({case (sum, count) => sum/count}).collect()
  }
}
