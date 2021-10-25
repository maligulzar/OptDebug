package examples.benchmarks.optdebugBaselineCoverage

import org.apache.spark.{SparkConf, SparkContext}


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
      logFile = "datasets/hdfs/grades-lineage/*"
    } else {
      println(s" Loading the arguments : ${args(0)} -- ${args(1)}")
      logFile = args(0)
      sparkConf.setMaster(args(1))
      sparkConf.setAppName("Student")
    } //set up spark context

  val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage

    ctx.textFile(logFile).zipWithIndex().filter(_._2 % 4 == 0).map(s=>s._1).map(line => {
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
      { case ((sum, count), next) => (sum + next, count + 1) },
      { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
    ).mapValues({ case (sum, count) => sum / count }).collect()
  }
}
