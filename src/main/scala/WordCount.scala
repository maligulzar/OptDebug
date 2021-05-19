
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import provenance.data.RoaringBitmapProvenance
import symbolicprimitives.{SymInt, SymString, Utils}

/**
  * Created by malig on 10/31/19.
  */
import scala.collection.mutable.BitSet

object WordCount {

 def main(args: Array[String]): Unit = {

//  var mySet: BitSet = BitSet(0, 1, 2);
//  var b1 = mySet
//  var a1 = b1.clone();
//  a1.add(100)
//  b1.add(5)
//  println(b1 + "---" + SizeEstimator.estimate(b1))
//  println(a1 + "---" + SizeEstimator.estimate(a1))
//  println(mySet + "---" + SizeEstimator.estimate(mySet))
//
//  val bitmap = new RoaringBitmap()
//  bitmap.add(Array(0,1,2,100).map(_.toInt): _*)
//  println(bitmap + "---" + SizeEstimator.estimate(new RoaringBitmapProvenance(bitmap)))
//  println(bitmap + "---" + SizeEstimator.estimate((bitmap)))
//


  val logFile = "/Users/malig/workspace/git/titian-2.1/bigdebug/README.md"
  val lc = new LineageContext(new SparkContext("local[4]", "wordcountTest"))

  lc.setCaptureLineage(true)

  // Job
  val file = lc.textFile(logFile, 2)
  val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
  val result = pairs.reduceByKey(_ + _)
  result.collectWithId().foreach(println)

  lc.setCaptureLineage(false)

  // Trace backward one record
  var linRdd = result.getLineage()
  linRdd = linRdd.filter(_ == 4294970851L)
  linRdd = linRdd.goBackAll()
  linRdd.show()

    //.collect().foreach(println)
  }
}
