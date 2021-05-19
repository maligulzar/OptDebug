package symbolicprimitives

import provenance.data.{DummyProvenance, Provenance}

/**
  * Created by malig on 4/29/19.
  */
case class SymString(override val value: String, p: Provenance = DummyProvenance.create()) extends SymAny(value, p) {

  setProvenance(getCallSite())

   def length: SymInt = {
     val mprov =  getCallSite()
     SymInt(value.length, newProvenance(mprov.cloneProvenance()))
  }

   def split(separator: Char): Array[SymString] = {
     val mprov =  getCallSite()
    value
      .split(separator)
      .map(s =>
         SymString(
          s, newProvenance(mprov.cloneProvenance())))
  }
  def split(regex: String): Array[SymString] = {
    split(regex, 0)
  }
  
  def split(regex: String, limit: Int): Array[SymString] = {
    val mprov =  getCallSite()
    value
      .split(regex, limit)
      .map(s =>
             SymString(
               s, newProvenance(mprov.cloneProvenance())))
  }
   def split(separator: Array[Char]): Array[SymString] = {
     val mprov =  getCallSite()
    value
      .split(separator)
      .map(s =>
         SymString(
          s,newProvenance(mprov.cloneProvenance())
        ))
  }

  def substring(arg0: SymInt): SymString = {
      val mprov =  getCallSite()
      SymString(value.substring(arg0.value), newProvenance(arg0.getProvenance(), mprov.cloneProvenance()))
  }

  def +(arg0: SymString): SymString = {
    val mprov =  getCallSite()
    SymString(value + arg0.value, newProvenance(arg0.getProvenance(), mprov.cloneProvenance()))
  }

  def substring(arg0: Int, arg1: SymInt): SymString = {
    val mprov =  getCallSite()
    SymString(value.substring(arg0, arg1.value), newProvenance(arg1.getProvenance(), mprov.cloneProvenance()))
  }
  def substring(arg0: SymInt, arg1: SymInt): SymString = {
    val mprov =  getCallSite()
    SymString(value.substring(arg0.value, arg1.value), newProvenance(arg0.getProvenance(), arg1.getProvenance() , mprov.cloneProvenance()))
  }

  def lastIndexOf(elem: Char): SymInt = {
    val mprov =  getCallSite()
    SymInt(value.lastIndexOf(elem), newProvenance(mprov.cloneProvenance()))
  }
  
  def trim(): SymString = {
    val mprov =  getCallSite()
    SymString(value.trim, newProvenance(mprov.cloneProvenance()))
  }


   def toInt: SymInt ={
    val mprov =  getCallSite()
    SymInt( value.toInt, newProvenance(mprov.cloneProvenance()))
  }

  def isEmpty: Boolean ={
    val mprov =  getCallSite()
    setProvenance(newProvenance(mprov.cloneProvenance()))
    value.isEmpty
  }

   def toFloat: SymFloat = {
     val mprov = getCallSite()
     SymFloat(value.toFloat, newProvenance(mprov.cloneProvenance()))
   }
   def toDouble: SymDouble ={
    SymDouble(value.toDouble , getProvenance())
  }

  // TODO: add configuration to track equality checks, e.g. if used as a key in a map.
  def equals(obj: SymString): Boolean = value.equals(obj.value)
  def eq(obj: SymString): Boolean = value.eq(obj.value)

}
