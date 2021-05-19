package refactor

import symbolicprimitives.SymString

import scala.collection.mutable
import scala.meta._

class ProvenanceInserter extends Transformer {

  // A stack of branch_predicates and a boolean indicating if in IF or ELSE
  val branch_predicates = mutable.Stack[(List[String], Boolean)]()
  var in_predicate = false
  var in_IF_body = false
  var capture_prov_dep = false

  override def apply(tree: Tree): Tree = tree match {

    case Term.Apply(name) => {
      name._1 match {
        case Term.Select(call) =>
          if (List("map",
                   "reduceByKey",
                   "aggregateByKey",
                   "flatmap",
                   "groupByKey",
                   "mapValues").contains(call._2.value)) {
            capture_prov_dep = true
            val t = name._2.map(s => apply(s).asInstanceOf[Term])
            capture_prov_dep = false
            return Term.Apply(apply(name._1).asInstanceOf[Term], t)
          }
        case _ =>
      }
      super.apply(tree)
    }
    case Term.If(branch) =>
      if (!capture_prov_dep) return tree
      println(branch)
      // Traverse the branch predicate
      in_predicate = true
      branch_predicates.push((List(), true))
      val pred = apply(branch._1)
      in_predicate = false

      // Traverse the IFbody
      val if_body = apply(branch._2)

      // Traverse the ELSEBody
      //  branch_predicates.update(0, (branch_predicates.top._1, false))
      val else_body = apply(branch._3)
      branch_predicates.pop()
      Term.If(pred.asInstanceOf[Term],
              if_body.asInstanceOf[Term],
              else_body.asInstanceOf[Term])

    case Lit.String(str) =>
      createSymTerm(Term.Name("SymString") ,tree)

    case Lit.Int(str) =>
      createSymTerm(Term.Name("SymInt") ,  tree)

    case Lit.Long(str) =>
      createSymTerm(Term.Name("SymLong") , tree)
      
    case Lit.Float(str) =>
      createSymTerm(Term.Name("SymFloat") ,  tree)

    case Lit.Double(str) =>
      createSymTerm(Term.Name("SymDouble") ,  tree)

    case Term.ApplyInfix(name) =>
      if (in_predicate) {
        println("In Term.infix " + name)
        apply(name._1)
        name._4.map(s => apply(s))
      }
      tree

    case Term.Name(name) =>
      if (in_predicate && name.matches("^[a-zA-Z_$][a-zA-Z_$0-9]*$")) {
        var (list, ifbody) = branch_predicates.pop()
        list = name :: list
        branch_predicates.push((list, ifbody))
      }
      tree

    case node =>
      super.apply(node)
  }

  def createSymTerm[T](symtype:Term, tree: Tree): Tree ={
    if(branch_predicates.length == 0) return tree
    if (branch_predicates.top._2) {
      val list = mergeNestedCalls()
      val t = q"$symtype(${tree.asInstanceOf[Lit]}, Utils.addProvDependency(List[Any](..$list)))"
      t
    } else tree
  }

  /**
    * Change s, ._1, ._2 type element access calls to s._1._2
    * */
  def mergeNestedCalls(): List[Term] = {
    var set = Set[Term]()
    var variable: String = ""
    // Search the entire stack till the first IF predicate
    // if (a) else if(b) else ===> a & b
     for ((list, _) <- branch_predicates) {
      for (a <- list) {
        if (a.matches("_[0-9]*")) {
          variable = '.' + a + variable
        } else {
          variable = a + variable
          set += variable.parse[Term].get
          variable = ""
        }
      }
    }
    set.toList
  }
}

object Insert {

  def main(args: Array[String]): Unit = {

    val transformer = new ProvenanceInserter()
    val program =
      """object StudentGradesOptDebug {
        |
        |  def fun1(input: Lineage[String]): RDD[(String, Float)] = {
        |    //show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description
        |    input
        |    input.map(line => {
        |      val arr = line.split(",")
        |      val (studentID, grade) = (arr(0), arr(2).toInt)
        |
        |      (studentID, grade)
        |    }).mapValues(grade => {
        |      if (grade >= 93) 4.0f
        |      else if (grade >= 90) 3.7f
        |      else if (grade >= 87) 3.3f
        |      else if (grade >= 83) 3.0f
        |      else if (grade >= 80) 2.7f
        |      else if (grade >= 77) 23f
        |      else if (grade >= 73) 2.0f
        |      else if (grade >= 70) 1.7f
        |      else if (grade >= 67) 1.3f
        |      else if (grade >= 65) 1.0f
        |      else 0.0f
        |    }).aggregateByKey((0.0f, 0f))(
        |      {case ((sum, count), next) => (sum + next, count+1)},
        |      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
        |    ).mapValues({case (sum, count) => sum/count})
        |  }}""".stripMargin
    val tree = program.parse[Source].get
    val transformed = transformer(tree)
    
    println("-" * 50)
    println(transformed)
    println("-" * 50)

  }

}
