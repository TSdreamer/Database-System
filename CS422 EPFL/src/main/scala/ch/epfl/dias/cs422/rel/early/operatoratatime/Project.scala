package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val inputCols:IndexedSeq[Column] = input.execute()
    val inputCols_t:IndexedSeq[RelOperator.Tuple] = inputCols.transpose

    lazy val evaluator: RelOperator.Tuple => RelOperator.Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

    inputCols_t.map(t => evaluator(t)).transpose

  }
}
