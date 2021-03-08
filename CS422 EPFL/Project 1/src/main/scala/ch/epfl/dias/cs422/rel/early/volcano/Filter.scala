package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def open(): Unit = {
    input.open()
  }

  lazy val e: Tuple => Any = eval(condition, input.getRowType)
/*
  override def next(): Tuple = input.next() match {
    case null => null
    case t:Tuple => e(t) match {
      case true => t
      case _ => next()
    }

    //println("end filter",current_t)
  }
*/

  override def next(): Tuple = {

    var current_t:Tuple = input.next()

    if(current_t != null){
      current_t = e(current_t) match {
        case true => current_t
        case _ => next()
      }
      return current_t
    }
    else null
    //println("end filter",current_t)
  }




  override def close(): Unit = {
    input.close()
  }
}
