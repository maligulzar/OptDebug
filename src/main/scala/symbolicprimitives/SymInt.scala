package symbolicprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.{DummyProvenance, Provenance}



case class SymInt(override val value: Int, p : Provenance = DummyProvenance.create()) extends SymAny(value, p) {

  setProvenance(getCallSite())
  def this(value: Int) = {
    this(value, DummyProvenance.create())
  }
  
  /**
    * Overloading operators from here onwards
    */
  def +(x: Int): SymInt = {
    val mprov = getCallSite()
    val d = value + x
    SymInt(d, newProvenance(mprov.cloneProvenance()))
  }

  def -(x: Int): SymInt = {
    val mprov = getCallSite()
    val d = value - x
    SymInt(d, newProvenance(mprov.cloneProvenance()))
  }

  def *(x: Int): SymInt = {
    val mprov = getCallSite()
    val d = value * x
    SymInt(d, newProvenance(mprov.cloneProvenance()))
  }

  def *(x: Float): SymFloat = {
    val mprov = getCallSite()
    val d = value * x
    SymFloat(d, newProvenance(mprov.cloneProvenance()))
  }


  def /(x: Int): SymDouble= {
    val mprov = getCallSite()
    val d = value / x
    SymDouble(d, newProvenance(mprov.cloneProvenance()) )
  }

  def /(x: Long): SymDouble= {
    val mprov = getCallSite()
    val d = value / x
    SymDouble(d, newProvenance(mprov.cloneProvenance()))
  }

  def +(x: SymInt): SymInt = {
    val mprov = getCallSite()
    SymInt(value + x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def -(x: SymInt): SymInt = {
    val mprov = getCallSite()
    SymInt(value - x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def *(x: SymInt): SymInt = {
    val mprov = getCallSite()
    SymInt(value * x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def /(x: SymInt): SymInt = {
    val mprov = getCallSite()
    SymInt(value / x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def %(x: Int): SymInt = {
    val mprov = getCallSite()
    SymInt(value % x, newProvenance(mprov.cloneProvenance()))
  }
  
  // Implementing on a need-to-use basis
  def toInt: SymInt = this
  def toDouble: SymDouble = { val mprov = getCallSite()
    SymDouble(value.toDouble, newProvenance(mprov.cloneProvenance()))}
  
  /**
    * Operators not supported yet
    */

  def ==(x: Int): Boolean = value == x

  def toByte: Byte = value.toByte

  def toShort: Short = value.toShort

  def toChar: Char = value.toChar

  

  def toLong: Long = value.toLong

  def toFloat: Float = value.toFloat

  //def toDouble: Double = value.toDouble

  def unary_~ : Int = value.unary_~

  def unary_+ : Int = value.unary_+

  def unary_- : Int = value.unary_-

  def +(x: String): String = value + x

  def <<(x: Int): Int = value << x

  def <<(x: Long): Int = value << x

  def >>>(x: Int): Int = value >>> x

  def >>>(x: Long): Int = value >>> x

  def >>(x: Int): Int = value >> x

  def >>(x: Long): Int = value >> x

  def ==(x: Byte): Boolean = value == x

  def ==(x: Short): Boolean = value == x

  def ==(x: Char): Boolean = value == x

  def ==(x: Long): Boolean = value == x

  def ==(x: Float): Boolean = value == x

  def ==(x: Double): Boolean = value == x

  def !=(x: Byte): Boolean = value != x

  def !=(x: Short): Boolean = value != x

  def !=(x: Char): Boolean = value != x

  def !=(x: Int): Boolean = value != x

  def !=(x: Long): Boolean = value != x

  def !=(x: Float): Boolean = value != x

  def !=(x: Double): Boolean = value != x

  def <(x: Byte): Boolean = value < x

  def <(x: Short): Boolean = value < x

  def <(x: Char): Boolean = value < x

  def <(x: Int): Boolean = value < x

  def <(x: Long): Boolean = value < x

  def <(x: Float): Boolean = value < x

  def <(x: Double): Boolean = value < x

  def <=(x: Byte): Boolean = value <= x

  def <=(x: Short): Boolean = value <= x

  def <=(x: Char): Boolean = value <= x

  def <=(x: Int): Boolean = value <= x

  def <=(x: Long): Boolean = value <= x

  def <=(x: Float): Boolean = value <= x

  def <=(x: Double): Boolean = value <= x

  def >(x: Byte): Boolean = value > x

  def >(x: Short): Boolean = value > x

  def >(x: Char): Boolean = value > x

  def >(x: Int): Boolean = value > x
  def >(x: SymInt): Boolean = value > x.value

  def >(x: Long): Boolean = value > x

  def >(x: Float): Boolean = value > x

  def >(x: Double): Boolean = value > x

  def >=(x: Byte): Boolean = value >= x

  def >=(x: Short): Boolean = value >= x

  def >=(x: Char): Boolean = value >= x

  def >=(x: Int): Boolean = value >= x

  def >=(x: Long): Boolean = value >= x

  def >=(x: Float): Boolean = value >= x

  def >=(x: Double): Boolean = value >= x

  def |(x: Byte): Int = value | x

  def |(x: Short): Int = value | x

  def |(x: Char): Int = value | x

  def |(x: Int): Int = value | x

  def |(x: Long): Long = value | x

  def &(x: Byte): Int = value & x

  def &(x: Short): Int = value & x

  def &(x: Char): Int = value & x

  def &(x: Int): Int = value & x

  def &(x: Long): Long = value & x

  def ^(x: Byte): Int = value ^ x

  def ^(x: Short): Int = value ^ x

  def ^(x: Char): Int = value ^ x

  def ^(x: Int): Int = value ^ x

  def ^(x: Long): Long = value ^ x

  def +(x: Byte): Int = value + x

  def +(x: Short): Int = value + x

  def +(x: Char): Int = value + x

  def +(x: Long): Long = value + x

  def +(x: Float): Float = value + x

  def +(x: Double): Double = value + x

  def -(x: Byte): Int = value - x

  def -(x: Short): Int = value - x

  def -(x: Char): Int = value - x

  def -(x: Long): Long = value - x

  def -(x: Float): Float = value - x

  def -(x: Double): Double = value - x

  def *(x: Byte): Int = value * x

  def *(x: Short): Int = value * x

  def *(x: Char): Int = value * x

  def *(x: Long): Long = value * x

  def *(x: Double): Double = value * x

  def /(x: Byte): Int = value / x

  def /(x: Short): Int = value / x

  def /(x: Char): Int = value / x

  def /(x: Float): Float = value / x

  def /(x: Double): Double = value / x

  def %(x: Byte): Int = value % x

  def %(x: Short): Int = value % x

  def %(x: Char): Int = value % x

  def %(x: Long): Long = value % x

  def %(x: Float): Float = value % x

  def %(x: Double): Double = value % x

}

object SymInt {
  implicit def ordering: Ordering[SymInt] = Ordering.by(_.value)
}
