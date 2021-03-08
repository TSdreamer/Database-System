package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val inputCols:IndexedSeq[Column] = input.execute()
    val inputCols_t:IndexedSeq[RelOperator.Tuple] = inputCols.transpose
    lazy val e: RelOperator.Tuple => Any = eval(condition, input.getRowType)
    var outputRows:List[RelOperator.Tuple] = Nil

    for (t <- inputCols_t){
      e(t) match {
        case true => outputRows = outputRows :+ t
        case _ =>
      }
    }
    outputRows.toIndexedSeq.transpose
  }
}
