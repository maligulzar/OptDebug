package provenance.data

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.Set



class OptSetProvenance(var bitmap: Set[String])
    extends DataStructureProvenance(bitmap) {
  override def _cloneProvenance(): OptSetProvenance =
    new OptSetProvenance(bitmap.clone())

  override def _merge(other: Provenance): this.type = {
    other match {
      case rbp: OptSetProvenance =>
        bitmap = bitmap ++ rbp.bitmap
      // Optional (but potentially unsafe/incorrect?) operation to pre-emptively free memory
      //rbp.bitmap.clear()
      case other =>
        throw new NotImplementedError(
          s"Unsupported RoaringBitmap merge provenance " +
            s"type! $other")
    }
    this
  }

  /** Returns number of provenance IDs. */
  override def count: Int = bitmap.size

  /** Returns estimate in serialization size, experimental. */
  override def estimateSize: Long = SizeEstimator.estimate(bitmap)

  private var serializationCount: Int = 0
  private var deserializationCount: Int = 0
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    serializationCount += 1
    //if (serializationCount > 1) println(s"TADA!: I've been serialized $serializationCount times
    // " + s"and have $count items!")
    //println(SizeEstimator.estimate(this))
    out.writeObject(bitmap)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    deserializationCount += 1
    bitmap = in.readObject().asInstanceOf[Set[String]]
  }

  override def toString: String = {
    var count = 0
    val printLimit = 10
    val iter = this.bitmap.iterator
    var buf = new StringBuilder("[")
    while (count < printLimit && iter.hasNext) {
      buf ++= iter.next().toString
      if (iter.hasNext) buf += ','
      count += 1
    }
    if (iter.hasNext) buf ++= s" ...(${this.bitmap.size - printLimit} more)"
    buf += ']'
    s"${this.getClass.getSimpleName}: ${buf.toString()}"
  }

  override def containsAll(other: Provenance): Boolean = {
    other match {
      case rbp: OptSetProvenance =>
        rbp.bitmap.map(bitmap.contains(_)).reduce(_ && _)
      case dummy: DummyProvenance => true
      case other =>
        throw new NotImplementedError(
          s"Unsupported RoaringBitmap containsAll check: $other")
    }
  }
}

object OptSetProvenance extends ProvenanceFactory {
  override def create(ids: Long*): OptSetProvenance = {
    if (ids.exists(_ > Int.MaxValue))
      // jteoh: Roaring64NavigableBitmap should be an option if this is required.
      throw new UnsupportedOperationException(
        s"At least one offset is greater than Int.Max which is not supported yet: $ids")
    var bitmap =  Set[String]()
    val m_name =  getMethodName()
    ids.map(e => bitmap += (e.toString +':'+ m_name))


    new OptSetProvenance(bitmap)
  }

  def getMethodName(): String = {
    val cls = Thread.currentThread
      .getStackTrace()
      .dropWhile { s =>
        s.getMethodName.startsWith("getCallSite") ||
        s.getClassName.startsWith("provenance") ||
        s.getClassName.startsWith("java.lang")
      }(0)
      cls.getClassName.replaceFirst("symbolicprimitives.Sym", "")+"."+cls.getMethodName.replaceAll("<init>" , "new()")
  }
}
