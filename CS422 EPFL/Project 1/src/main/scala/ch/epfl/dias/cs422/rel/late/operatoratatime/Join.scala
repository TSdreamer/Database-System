package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  val leftCols: IndexedSeq[VIDs] = left.execute()
  val rightCols: IndexedSeq[VIDs] = right.execute()

  lazy val leftInput = left.evaluators()
  lazy val rightInput = right.evaluators()

  def execute(): IndexedSeq[VIDs] = {
    val leftKeys:IndexedSeq[Int] = getLeftKeys
    val rightKeys:IndexedSeq[Int] = getRightKeys

    var outputList:IndexedSeq[VIDs] = IndexedSeq.empty
    val hashLeftTable:Map[IndexedSeq[RelOperator.Elem],List[VIDs]] = hashTableBuild(leftKeys)
    if(leftCols == IndexedSeq.empty || rightCols == IndexedSeq.empty){
      outputList
    }
    else {
      outputList = joinTable(rightKeys,hashLeftTable)
      outputList
    }
  }

  // Build a hash left table with {keys-> vids}
  def hashTableBuild(leftKeys:IndexedSeq[Int]):Map[IndexedSeq[RelOperator.Elem],List[VIDs]] = {
    val hashTable:Map[IndexedSeq[RelOperator.Elem],List[VIDs]] = Map()
    if(leftCols == IndexedSeq.empty){
      Map()
    }
    else {
      for (vids <- leftCols) {
        val row = leftInput(vids)
        val leftKeyValue: IndexedSeq[RelOperator.Elem] = leftKeys.map(leftKey => row(leftKey))
        if (hashTable.contains(leftKeyValue)) {
          val oldValue: List[VIDs] = hashTable.getOrElse(leftKeyValue, null)
          hashTable.update(leftKeyValue, oldValue :+ vids)
        }
        else {
          hashTable.put(leftKeyValue, List(vids))
        }
      }
      hashTable
    }
  }

  def joinTable(rightKeys:IndexedSeq[Int],hashTable:Map[IndexedSeq[RelOperator.Elem],List[VIDs]]):IndexedSeq[VIDs] = {
    var outputList:IndexedSeq[VIDs] = IndexedSeq()
    if (hashTable.isEmpty) outputList
    else {
      for(vids <- rightCols){
        val row:RelOperator.Tuple = rightInput(vids)
        val rightKeyValue:IndexedSeq[RelOperator.Elem] = rightKeys.map(rightKey => row(rightKey))
        if(hashTable.contains(rightKeyValue)){
          outputList ++= hashTable.getOrElse(rightKeyValue,null).map(t => IndexedSeq(t , vids)).toIndexedSeq
        }
      }
      outputList
    }
  }


  private lazy val evals = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)

  override def evaluators(): LazyEvaluatorRoot = evals
}
