package symbolicprimitives

import provenance.data.{DummyProvenance, Provenance}

/**
  * Created by malig on 4/25/19.
  */

case class SymFloat(override val value: Float, p:Provenance = DummyProvenance.create()) extends SymAny(value, p){
  /**
    * Overloading operators from here onwards
    */
  setProvenance(newProvenance(getCallSite().cloneProvenance()))



  def +(x: Float): SymFloat = {
    val mprov = getCallSite()
    val d = value + x
    SymFloat(d, newProvenance(mprov.cloneProvenance()))
  }

  def -(x: Float): SymFloat = {
    val mprov = getCallSite()
    val d = value - x
    SymFloat(d, newProvenance(mprov.cloneProvenance()))
  }

  def *(x: Float): SymFloat = {
    val mprov = getCallSite()
    val d = value * x
    SymFloat(d, newProvenance(mprov.cloneProvenance()))

  }

  def /(x: Float): SymFloat = {
    val mprov = getCallSite()
    val d = value / x
    SymFloat(d, newProvenance(mprov.cloneProvenance()))
  }

  def +(x: SymFloat): SymFloat = {
    val mprov = getCallSite()
    SymFloat(value + x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def +(x: SymDouble): SymDouble = {
    val mprov = getCallSite()
    SymDouble(value + x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def -(x: SymFloat): SymFloat = {
    val mprov = getCallSite()
    SymFloat(value - x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def *(x: SymFloat): SymFloat = {
    val mprov = getCallSite()
    SymFloat(value * x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))

  }

  def /(x: SymFloat): SymFloat = {
    val mprov = getCallSite()
    SymFloat(value / x.value, newProvenance(x.getProvenance(), mprov.cloneProvenance()))
  }

  def >(x: Float): Boolean = {
    val mprov = getCallSite()
    setProvenance(newProvenance(mprov.cloneProvenance()))
    return value > x
  }

  // Incomplete comparison operators - see discussion in SymDouble on provenance
  def >(x: SymFloat): Boolean = {
    // mergeProvenance(x.getProvenance())
    return value > x.value
  }
  
  
  /**
    * Operators not Supported Symbolically yet
    **/
  override def toString: String =
    value.toString + s""" (Most Influential Input Offset: ${getProvenance()})"""
//
//  def toByte: Byte = value.toByte
//
//  def toShort: Short = value.toShort
//
//  def toChar: Char = value.toChar
//
//  def toInt: Int = value.toInt
//
//  def toLong: Long = value.toLong
//
//  def toFloat: Float = value.toFloat
//
//  def toDouble: Double = value.toDouble
//
//  def unary_~ : Int = value.unary_~
//
//  def unary_+ : Int = value.unary_+
//
//  def unary_- : Int = value.unary_-
//
//  def +(x: String): String = value + x
//
//  def <<(x: Int): Int = value << x
//
//  def <<(x: Long): Int = value << x
//
//  def >>>(x: Int): Int = value >>> x
//
//  def >>>(x: Long): Int = value >>> x
//
//  def >>(x: Int): Int = value >> x
//
//  def >>(x: Long): Int = value >> x
//
//  def ==(x: Byte): Boolean = value == x
//
//  def ==(x: Short): Boolean = value == x
//
//  def ==(x: Char): Boolean = value == x
//
//  def ==(x: Long): Boolean = value == x
//
//  def ==(x: Float): Boolean = value == x
//
//  def ==(x: Double): Boolean = value == x
//
//  def !=(x: Byte): Boolean = value != x
//
//  def !=(x: Short): Boolean = value != x
//
//  def !=(x: Char): Boolean = value != x
//
//  def !=(x: Int): Boolean = value != x
//
//  def !=(x: Long): Boolean = value != x
//
//  def !=(x: Float): Boolean = value != x
//
//  def !=(x: Double): Boolean = value != x
//
//  def <(x: Byte): Boolean = value < x
//
//  def <(x: Short): Boolean = value < x
//
//  def <(x: Char): Boolean = value < x
//
//  def <(x: Int): Boolean = value < x
//
//  def <(x: Long): Boolean = value < x
//
//  def <(x: Float): Boolean = value < x
//
//  def <(x: Double): Boolean = value < x
//
//  def <=(x: Byte): Boolean = value <= x
//
//  def <=(x: Short): Boolean = value <= x
//
//  def <=(x: Char): Boolean = value <= x
//
//  def <=(x: Int): Boolean = value <= x
//
//  def <=(x: Long): Boolean = value <= x
//
//  def <=(x: Float): Boolean = value <= x
//
//  def <=(x: Double): Boolean = value <= x
//
//  def >(x: Byte): Boolean = value > x
//
//  def >(x: Short): Boolean = value > x
//
//  def >(x: Char): Boolean = value > x
//
//  def >(x: Int): Boolean = value > x
//
//  def >(x: Long): Boolean = value > x
//
//  def >(x: Float): Boolean = value > x
//
//  def >(x: Double): Boolean = value > x
//
//  def >=(x: Byte): Boolean = value >= x
//
//  def >=(x: Short): Boolean = value >= x
//
//  def >=(x: Char): Boolean = value >= x
//
//  def >=(x: Int): Boolean = value >= x
//
//  def >=(x: Long): Boolean = value >= x
//
//  def >=(x: Float): Boolean = value >= x
//
//  def >=(x: Double): Boolean = value >= x
//
//  def |(x: Byte): Int = value | x
//
//  def |(x: Short): Int = value | x
//
//  def |(x: Char): Int = value | x
//
//  def |(x: Int): Int = value | x
//
//  def |(x: Long): Long = value | x
//
//  def &(x: Byte): Int = value & x
//
//  def &(x: Short): Int = value & x
//
//  def &(x: Char): Int = value & x
//
//  def &(x: Int): Int = value & x
//
//  def &(x: Long): Long = value & x
//
//  def ^(x: Byte): Int = value ^ x
//
//  def ^(x: Short): Int = value ^ x
//
//  def ^(x: Char): Int = value ^ x
//
//  def ^(x: Int): Int = value ^ x
//
//  def ^(x: Long): Long = value ^ x
//
//  def +(x: Byte): Int = value + x
//
//  def +(x: Short): Int = value + x
//
//  def +(x: Char): Int = value + x
//
//  def +(x: Long): Long = value + x
//
//  def +(x: Float): Float = value + x
//
//  def +(x: Double): Double = value + x
//
//  def -(x: Byte): Int = value - x
//
//  def -(x: Short): Int = value - x
//
//  def -(x: Char): Int = value - x
//
//  def -(x: Long): Long = value - x
//
//  def -(x: Float): Float = value - x
//
//  def -(x: Double): Double = value - x
//
//  def *(x: Byte): Int = value * x
//
//  def *(x: Short): Int = value * x
//
//  def *(x: Char): Int = value * x
//
//  def *(x: Long): Long = value * x
//
//  def *(x: Float): Float = value * x
//
//  def *(x: Double): Double = value * x
//
//  def /(x: Byte): Int = value / x
//
//  def /(x: Short): Int = value / x
//
//  def /(x: Char): Int = value / x
//
//  def /(x: Long): Long = value / x
//
//  def /(x: Float): Float = value / x
//
//  def /(x: Double): Double = value / x
//
//  def %(x: Byte): Int = value % x
//
//  def %(x: Short): Int = value % x
//
//  def %(x: Char): Int = value % x
//
//  def %(x: Int): Int = value % x
//
//  def %(x: Long): Long = value % x
//
//  def %(x: Float): Float = value % x
//
//  def %(x: Double): Double = value % x

}