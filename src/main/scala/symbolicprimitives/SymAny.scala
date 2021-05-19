package symbolicprimitives

import provenance.data.Provenance

abstract class SymAny[T <: Any](val value: T, p: Provenance) extends SymBase(p) {
  def toSymString: SymString = {
    SymString(value.toString, getProvenance())
  }

  def getCallSite() : Provenance = {
    val loc = Thread.currentThread.getStackTrace()
      .dropWhile{s =>
       (s.getClassName.startsWith("symbolicprimitives") ||
        s.getClassName.startsWith("provenance") ||
        s.getClassName.startsWith("java.lang"))
      }(0)
      .getLineNumber
    val provCreatorFn = Provenance.createFn()
    val prov = provCreatorFn(Seq(loc))
    newProvenance(prov)
  }

  override def hashCode(): Int = value.hashCode()
  
  override def equals(obj: scala.Any): Boolean =
    obj match {
      case x: SymAny[_] => value.equals(x.value)
      case _ => value.equals(obj)
    }
  
  override def toString: String = s"${this.getClass.getSimpleName}($value, ${getProvenance()})"
  
  
  // jteoh Disabled: These could actually be implicits in the symbase class too, but I'm hesitant
  // to do so because of scoping and incomplete knowledge on implicits.
  //  /** Creates new SymInt with current object provenance */
  //  def sym(v: Int): SymInt = SymInt(v, p)
  //
  //  // TODO: support Long type
  //  /** Creates new SymLong with current object provenance */
  //  def sym(v: Long): SymLong = new SymLong(v.toInt, p)
  //
  //  /** Creates new SymDouble with current object provenance */
  //  def sym(v: Double): SymDouble = SymDouble(v, p)
  //
  //  /** Creates new SymFloat with current object provenance */
  //  def sym(v: Float): SymFloat = SymFloat(v, p)
  //
  //  /** Creates new SymString with current object provenance */
  //  def sym(v: String): SymString = SymString(v, p)
}
object SymAny {
}
