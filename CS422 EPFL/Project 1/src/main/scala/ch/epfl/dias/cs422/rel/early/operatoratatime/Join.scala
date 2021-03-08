package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val leftCols: IndexedSeq[Column] = left.execute()
    val rightCols: IndexedSeq[Column] = right.execute()
    val leftKeys:IndexedSeq[Int] = getLeftKeys
    var hashLeftTable:Map[IndexedSeq[RelOperator.Elem],List[RelOperator.Tuple]] = Map()
    val rightKeys:IndexedSeq[Int] = getRightKeys
    var outputList:List[RelOperator.Tuple] = Nil

    val leftCols_t:IndexedSeq[RelOperator.Tuple] = leftCols.transpose
    val rightCols_t:IndexedSeq[RelOperator.Tuple] = rightCols.transpose


    for (row <- leftCols_t){
      val leftKeyValue:IndexedSeq[RelOperator.Elem] = leftKeys.map(leftKey => row(leftKey))
      if(hashLeftTable.contains(leftKeyValue)){
        val oldValue:List[RelOperator.Tuple] = hashLeftTable.getOrElse(leftKeyValue,null)
        hashLeftTable.update(leftKeyValue, oldValue :+ row)
      }
      else{
        hashLeftTable.put(leftKeyValue,List(row))
      }

    }
    if (hashLeftTable.isEmpty) IndexedSeq.empty
    else {
      for(row <- rightCols_t){
        val rightKeyValue:IndexedSeq[RelOperator.Elem] = rightKeys.map(rightKey => row(rightKey))
        if(hashLeftTable.contains(rightKeyValue)){
          outputList = outputList ++ hashLeftTable.getOrElse(rightKeyValue,null).map(t => (t ++ row))
        }
      }
      outputList.toIndexedSeq.transpose
    }

  }
}
