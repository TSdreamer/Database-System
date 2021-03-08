package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  val leftKeys:IndexedSeq[Int] = getLeftKeys
  var hashLeftTable:Map[IndexedSeq[RelOperator.Elem],List[Tuple]] = Map()
  val rightKeys:IndexedSeq[Int] = getRightKeys
  var flagMulTuple:Int = 0
  var tempRightTuple:Tuple = null


  override def open(): Unit = {
    left.open()
    right.open()

    buildHashTable()
    //println(condition,hashLeftTable)
  }

  def buildHashTable():Unit = {
    var currentLeftTuple:Tuple = left.next()
    while(currentLeftTuple != null){
      val leftKeyValue = leftKeys.map(leftKey => currentLeftTuple(leftKey))
      if(hashLeftTable.contains(leftKeyValue)){
        val oldValue:List[Tuple] = hashLeftTable.getOrElse(leftKeyValue,null)
        hashLeftTable.update(leftKeyValue, oldValue :+ currentLeftTuple)
      }
      else {
        hashLeftTable.put(leftKeyValue,List(currentLeftTuple))
      }
      currentLeftTuple = left.next()
    }
  }

  override def next(): Tuple = {
    var currentRightTuple:Tuple = flagMulTuple match {
      case 0 => right.next()
      case _ => tempRightTuple
    }
    if (currentRightTuple == null || hashLeftTable.isEmpty){
      null
    }
    else {

      //println(rightKeyValue)
      while(currentRightTuple!=null){
        val rightKeyValue:IndexedSeq[RelOperator.Elem] = rightKeys.map(rightKey => currentRightTuple(rightKey))
        val listLeftTuple:List[Tuple] = hashLeftTable.getOrElse(rightKeyValue,null)
        //println(currentRightTuple)
        if(hashLeftTable.contains(rightKeyValue)){

          tempRightTuple = currentRightTuple

          if(flagMulTuple < listLeftTuple.length){
            flagMulTuple += 1
            //println("join",flagMulTuple,listLeftTuple(flagMulTuple - 1) ++ currentRightTuple)
            return listLeftTuple(flagMulTuple - 1) ++ currentRightTuple
          }
          else {
            flagMulTuple = 0
            //println("join",listLeftTuple.last ++ currentRightTuple)
            currentRightTuple = right.next()
          }
        }
        else currentRightTuple = right.next()
      }
      //println("end join",condition)
      null
    }

  }

  override def close(): Unit = {
    left.close()
    right.close()
  }

}
