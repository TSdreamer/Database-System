package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode
//import scala.collection.mutable.Map

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  val leftKeys:IndexedSeq[Int] = getLeftKeys
  var hashLeftTable:Map[IndexedSeq[RelOperator.Elem],List[Tuple]] = Map()
  val rightKeys:IndexedSeq[Int] = getRightKeys
  var rightTable:List[Tuple] = Nil
  var joinTable:List[Tuple] = Nil
  var numBlock:Int = 0
  var numRight:Int = 0
  var returnLine:Int = 0

  override def open(): Unit = {
    left.open()
    right.open()

    // build the hashtable for the left
    buildHashTable()

    // build the table for the right
    var currentRightTupleList:IndexedSeq[Tuple] = right.next()
    while (currentRightTupleList != null){
      rightTable ++= currentRightTupleList.toList
      currentRightTupleList = right.next()
    }

    buildJoinTable()
  }

  def buildHashTable():Unit = {
    var currentLeftTupleList:IndexedSeq[Tuple] = left.next()
    var leftTupleList:List[Tuple] = Nil
    while(currentLeftTupleList != null){
      leftTupleList = leftTupleList ++ currentLeftTupleList.toList
      currentLeftTupleList = left.next()
    }
    hashLeftTable = leftTupleList.groupBy(tuple => leftKeys.map(tuple))
  }


  def buildJoinTable(): Unit = {
    if(rightTable == Nil || hashLeftTable.isEmpty){
      joinTable = Nil
    }
    else {
      while (numRight < rightTable.length){
        val currentRightTuple:Tuple = rightTable(numRight)
        val rightKeyValue:IndexedSeq[RelOperator.Elem] = rightKeys.map(rightKey => currentRightTuple(rightKey))
        if(hashLeftTable.contains(rightKeyValue)){
          val listLeftTuple:List[Tuple] = hashLeftTable.getOrElse(rightKeyValue,null)
          joinTable ++= listLeftTuple.map(t => (t ++ currentRightTuple))
        }
        else {
          joinTable = joinTable
        }
        numRight += 1
      }
    }
  }


  override def next(): Block = {
    //val numRightMax:Int = rightTable.length
    //var inBlock:Int = 0
    var returnTupleList:List[Tuple] = Nil
    //var flag:Int = 0

    if(joinTable == Nil){
      return null
    }
    else {
      val rowsValuable:List[Int] = (returnLine until math.min(returnLine + blockSize,joinTable.length)).toList
      if(returnLine < joinTable.length){
        returnLine += rowsValuable.length
        returnTupleList = rowsValuable.map(i => joinTable(i))
        returnTupleList.toIndexedSeq
      }
      else {
        return null
      }

    }
  }

  override def close(): Unit = {
    left.close()
    right.close()
    numBlock = 0
    numRight = 0
  }
}
