package ch.epfl.dias.cs422.rel.early.blockatatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected (input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {

  override def open(): Unit = {
    input.open()
  }

  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def next(): Block = {

    val currentTupleList:IndexedSeq[Tuple] = input.next()

    if (currentTupleList!=null){
      currentTupleList.map(t => evaluator(t))
    }
    else{
      currentTupleList
    }


    //var returnTupleList:List[Tuple] = Nil
    //var restTupleList:List[Tuple] = Nil
    /*
    if(blockSize == 0){
      return null
    }
    */

  }

  override def close(): Unit = {
    input.close()
  }
}
