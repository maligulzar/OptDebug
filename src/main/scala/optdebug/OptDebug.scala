package optdebug

import java.util.Calendar
import java.util.logging.{FileHandler, Level, LogManager, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD
import provenance.data.{BitSetProvenance, OptSetProvenance, Provenance}
import provenance.rdd.ProvenanceRDD
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymAny, SymInt, SymString, Utils}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class OptDebug(lc: LineageContext, logFile: String = "tempdata" , perParitionSize:Int = 10000 ,SELECTIVITY_FACTOR_SUCCESS:Int=2, SELECTIVITY_FACTOR_FAILURE:Int=2  ) {

//  val SELECTIVITY_FACTOR_SUCCESS = 2 // number of correct records to trace
  //val SELECTIVITY_FACTOR_FAILURE = 2 // number of incorrect records to trace

  val lm: LogManager = LogManager.getLogManager
  val fh: FileHandler = new FileHandler("myLog")

  // Runtimes in ms
  var withLineageFirstRun = -1L
  var lineageQueryTime = -1L
  var lineageSize = -1L
  var totalPassRecordsTraced = -1L
  var totalFailRecordsTraced = -1L
  var runtimeWithTaint = -1L
  var nanoToEvalUnit: Long = 1000000 //milliseconds
  var total_faults_found = -1
  var inputSize = -1L

  var MIN_LINEAGE_PARTITION_SIZE  = 35
  var _counts: Map[String, (Int, Int)] = Map().withDefaultValue((0, 0)) // failure , success
  /**
    *
    * Input RDD initiated with Lineage Context
    * */
  def runWithOptDebug[T, O](input: String,
                            f1: Lineage[String] => RDD[T],
                            f2: ProvenanceRDD[SymString] => ProvenanceRDD[O],
                            test: Option[T => Boolean],
                            logging: Boolean = false): Unit = {
    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.setLevel(Level.INFO)

    /************************** Time Logging **************************/
    val jobStartTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val jobStartTime = System.nanoTime()
    logger.log(Level.INFO, "Job starts at " + jobStartTimestamp)

    /************************** Time Logging **************************/
    val inputRDD = lc.textFile(input)
    val output = f1(inputRDD).asInstanceOf[Lineage[T]]
    val out = output.collectWithId()
    if (logging) out.foreach(println)

    /** ************************ Time Logging *************************/
    println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    val jobEndTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val jobEndTime = System.nanoTime()
    withLineageFirstRun = (jobEndTime - jobStartTime) / nanoToEvalUnit
    logger.log(Level.INFO, "Job ends at " + jobEndTimestamp)
    logger.log(
      Level.INFO,
      "Job span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

    /** ************************Time Logging* *************************/
    lc.setCaptureLineage(false)
    Thread.sleep(1000)

    var testimpl: T => Boolean = null;
    if (test.isDefined) {
      testimpl = test.get
    } else {
      logger.log(Level.SEVERE, "Test function not defined")
    }

    val list = selectRecordForOptTainting(out, testimpl)
    if (logging) list.foreach(println)

    /************************** Time Logging **************************/
    val lineageStartTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val lineageStartTime = System.nanoTime()
    logger.log(Level.INFO, "Lineage job starts at " + lineageStartTimestamp)

    /************************** Time Logging **************************/
    var linRdd = output.getLineage()
    linRdd.collect

    linRdd = linRdd.filter { l =>
      list.contains(l)
    }
    linRdd = linRdd.goBackAll()

    val mappedRDD = linRdd.show(false).toRDD

    lineageSize = mappedRDD.count()

    /************************** Time Logging **************************/
    println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    val lineageEndTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val lineageEndTime = System.nanoTime()
    lineageQueryTime = (lineageEndTime - lineageStartTime) / nanoToEvalUnit
    logger.log(Level.INFO, "Lineage job ends at " + lineageEndTimestamp)
    logger.log(
      Level.INFO,
      "Lineage job span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

    /************************** Time Logging **************************/
    if (logging) mappedRDD.collect().foreach(println)

    // Deletes directory of exists
    import scala.reflect.io.Directory
    import java.io.File
    val output_directory = logFile + "-" + "lineage"
    val directory = new Directory(new File(output_directory))
    directory.deleteRecursively()
    mappedRDD.repartition(getPartitionsNum(lineageSize)).saveAsTextFile(output_directory)


    /**
      *
      *
      * Starting the tainted job with operation provenance
      *
      * */
    Utils.setUDFAwareDefaultValue(true)
    val conf = lc.sparkContext.getConf
    lc.sparkContext.stop()
    Thread.sleep(1000)
    val nsc= new SparkContext(conf)
    val scdp = new SparkContextWithDP(nsc)

    /************************** Time Logging **************************/
    val codeDebuggingStartTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val codeDebuggingStartTime = System.nanoTime()
    logger.log(Level.INFO,
               " TaintAnalysis job starts at " + codeDebuggingStartTimestamp)

    /************************** Time Logging **************************/

    val taintedInputRdd = scdp.textFileSymbolic(output_directory + "/*")
    val taintedOutput = f2(taintedInputRdd).collect()

    /************************** Time Logging **************************/
    val codeDebuggingEndTime = System.nanoTime()
    val codeDebuggingEndTimestamp =
      new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    runtimeWithTaint = (codeDebuggingEndTime - codeDebuggingStartTime) / nanoToEvalUnit
    logger.log(
      Level.INFO,
      "DeltaDebugging (unadjusted) + L  ends at " + codeDebuggingEndTimestamp)
    logger.log(
      Level.INFO,
      "DeltaDebugging (unadjusted)  + L takes " + (codeDebuggingEndTime - codeDebuggingStartTime) / 1000 + " milliseconds")

    /************************** Time Logging **************************/
    //TODO: Remove the print later
    if (logging) taintedOutput.foreach(println)

    //Failures
    val taints = extractTaints(taintedOutput, testimpl, true)
    taints.toList.flatten.foreach {
      case s: BitSetProvenance =>
        s.bitmap.foreach(
          e =>
            _counts += (e.toString -> (_counts(e.toString)._1 + 1, _counts(
              e.toString)._2)))
      case s: OptSetProvenance =>
        s.bitmap.foreach(
          e =>
            _counts += (e.toString -> (_counts(e.toString)._1 + 1, _counts(
              e.toString)._2)))
      case _ =>
        throw new UnsupportedOperationException(
          "Not Support operation extract Taints on non-tuple output")
    }

    // Success
    val taints_success = extractTaints(taintedOutput, testimpl, false)
    taints_success.toList.flatten.foreach {
      case s: BitSetProvenance =>
        s.bitmap.foreach(
          e =>
            _counts += (e.toString -> (_counts(e.toString)._1, _counts(
              e.toString)._2 + 1)))
      case s: OptSetProvenance =>
        s.bitmap.foreach(
          e =>
            _counts += (e.toString -> (_counts(e.toString)._1, _counts(
              e.toString)._2 + 1)))
      case _ =>
        throw new UnsupportedOperationException(
          "Not Support operation extract Taints on non-tuple output")
    }

    nsc.stop()
    val t_faults= findFaultLocationWithTarantula()
    val oc_faults= findFaultLocationWithOchiai()
    val s_faults= findFaultLocationWithSpectra()
    val op_faults= findFaultLocationWithOP2()
    val b_faults= findFaultLocationWithBarinel()
    val log_score =
      s"""
         |-------------------------------------------------------------
         |Tarantula , Ochiai ,  Spectra , OP2  ,  Barinel
         |$t_faults , $oc_faults  , $s_faults , $op_faults  , $b_faults
         |-------------------------------------------------------------
         |*******************************************************************
         |""".stripMargin

    printCollectedMetris(log_score)

  }

  def printCollectedMetris(csv_score:String) = {
    println(
      s"""*******************************************************************
         | Total Input Size               : $inputSize
         | Total Job Time With Lineage    : $withLineageFirstRun
         | Total Lineage Query Time       : $lineageQueryTime
         | Total Lineage Trace Size       : $lineageSize
         | Total Failed Tests             : $totalFailRecordsTraced
         | Total Passed Tests             : $totalPassRecordsTraced
         | Total Taint Job Time           : $runtimeWithTaint
         |
         |$inputSize,$withLineageFirstRun,$lineageQueryTime,$lineageSize,$totalFailRecordsTraced,$totalPassRecordsTraced,$runtimeWithTaint
         |
         |$csv_score
         |""".stripMargin)

  }
  def findFaultLocationWithBarinel(): Int = {
    val fault_list = _counts
      .mapValues { s =>
        val fail = s._1.toFloat
        val pass = s._2.toFloat
        val score = 1- (pass / (pass + fail))
        score
      }
      .toList
      .sortWith(_._2 > _._2)
    print("*" * 20)
    print("Barniel Score")
    println("*" * 20)
    fault_list.foreach(println)
    println("*" * 40)
    val max = fault_list(0)
    fault_list.filter(s => s._2 == max._2).size
  }

  def findFaultLocationWithOP2(): Int = {

    val total_pass =totalPassRecordsTraced

    val fault_list = _counts
      .mapValues { s =>
        val fail = s._1.toFloat
        val pass = s._2.toFloat
        val score = fail  - (pass / (total_pass +1))
        score
      }
      .toList
      .sortWith(_._2 > _._2)
    print("*" * 20)
    print("OP2 Score")
    println("*" * 20)
    fault_list.foreach(println)
    println("*" * 40)
    val max = fault_list(0)
    fault_list.filter(s => s._2 == max._2).size

  }

  def findFaultLocationWithOchiai(): Int = {
    val total_fails = totalFailRecordsTraced

    val fault_list = _counts
      .mapValues { s =>
        val fail = s._1.toFloat
        val pass = s._2.toFloat
        val score = fail/ Math.sqrt(total_fails * ( fail+ pass))
        score
      }
      .toList
      .sortWith(_._2 > _._2)
    print("*" * 20)
    print("Ochiai Score")
    println("*" * 20)
    fault_list.foreach(println)
    println("*" * 40)
    val max = fault_list(0)
    fault_list.filter(s => s._2 == max._2).size

  }

  def findFaultLocationWithTarantula(): Int = {
    val total_fails = totalFailRecordsTraced

    val total_pass = totalPassRecordsTraced

    val fault_list = _counts
      .mapValues { s =>
        val fail = s._1.toFloat
        val pass = s._2.toFloat
        val score = (fail / total_fails) / ((fail / total_fails) + (pass / total_pass))
        score
      }
      .toList
      .sortWith(_._2 > _._2)
    print("*" * 20)
    print("Tarantula Score")
    println("*" * 20)
    fault_list.foreach(println)
    println("*" * 40)
    val max = fault_list(0)
     fault_list.filter(s => s._2 == max._2).size

  }
  def findFaultLocationWithSpectra(): Int = {
    val total_fails = _counts.map(_._2._1).reduce(_ + _).toFloat

    val total_pass = _counts.map(_._2._2).reduce(_ + _).toFloat

    val fault_list = _counts
      .mapValues { s =>
        val fail = s._1.toFloat
        val pass = s._2.toFloat
        val score = (fail / total_fails) / ((fail / total_fails) + (pass / total_pass))
        score
      }
      .toList
      .sortWith(_._2 > _._2)
    print("*" * 20)
    print("Custom Score")
    println("*" * 20)
    fault_list.foreach(println)
    println("*" * 40)
    val max = fault_list(0)
     fault_list.filter(s => s._2 == max._2).size

  }
  def selectRecordForOptTainting[T](list: Array[(T, Long)],
                                    test: T => Boolean): ListBuffer[Long] = {
    var correct = new ArrayBuffer[Long]()
    var failure = new ArrayBuffer[Long]()

    list.foreach(s => if (test(s._1)) correct += s._2 else failure += s._2)

    var retList = new ListBuffer[Long]()
    var size = 0
    if (failure.length < SELECTIVITY_FACTOR_FAILURE)
      failure.foreach(retList += _)
    else
      retList = retList ++ scala.util.Random
        .shuffle(failure)
        .take(SELECTIVITY_FACTOR_FAILURE)

    totalFailRecordsTraced = retList.size - size
    size = retList.size

    if (correct.length < SELECTIVITY_FACTOR_SUCCESS)
      correct.foreach(retList += _)
    else
      retList = retList ++ scala.util.Random
        .shuffle(correct)
        .take(SELECTIVITY_FACTOR_SUCCESS)

    totalPassRecordsTraced = retList.size - size

    retList
  }

  import shapeless._
  import ops.tuple.FlatMapper
  import syntax.std.tuple._
  import test._

  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T] = at[T](Tuple1(_))
  }
  object flatten extends LowPriorityFlatten {
    implicit def caseTuple[P <: Product, O](
        implicit lfm: Lazy[FlatMapper.Aux[P, flatten.type, O]]) =
      at[P](lfm.value(_))
  }

  def extractTaints[T](
      output: Array[_],
      test: T => Boolean,
      getFailureTaints: Boolean = true): Array[List[Provenance]] = {
    val listOfProv = output
      .filter {
        case s: Product =>
          val strippedOutput = flatItemsInTupleWithStrippedProvenance(s)
          strippedOutput match {
            case o: T => if (getFailureTaints) !test(o) else test(o)
            case _ =>
              throw new UnsupportedOperationException("Test type mismatch")
          }
        case _ =>
          throw new UnsupportedOperationException(
            "Not Support operation extract Taints on non-tuple output")
      }
      .map {
        case s: Product =>
          retrieveProvenance(s)
      }
    listOfProv
  }

  def flatItemsInTupleWithStrippedProvenance(v: Product): Product = {
    val list = v.productIterator.map {
      case (item: SymAny[_]) => stripProvenance(item)
      case (item: Product)   => flatItemsInTupleWithStrippedProvenance(item)
      case (item: Any)       => item

    }.toList
    (list(0), list(1))
  }

  def retrieveProvenance(v: Product): List[Provenance] = {
    val list = v.productIterator
      .map {
        case (item: SymAny[_]) => List(item.getProvenance())
        case (item: Product)   => retrieveProvenance(item)
        case (item: Any)       => List()
      }
      .toList
      .flatten
    list
  }

  def stripProvenance[T](v: SymAny[T]): T = {
    v.value
  }

  def getPartitionsNum(size:Long): Int ={
    Math.max(24, size /perParitionSize).toInt
  }

}
