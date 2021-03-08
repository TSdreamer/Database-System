package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  var tupleList:List[Tuple] = Nil
  var currentTupleList:IndexedSeq[Tuple] = null
  //var numBlock:Int = 0
  var rowIndex:Int = 0

  override def open(): Unit = {
    input.open()
    currentTupleList = input.next()
    while (currentTupleList!= null){
      tupleList = tupleList ++ currentTupleList.toList
      currentTupleList = input.next()
    }

  }

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Block = {
    var inBlock:Int = 0
    //val numBlockMax:Int = tupleList.length / blockSize +1
    //val lastBlock:Int = tupleList.length % blockSize
    var returnTupleList:List[Tuple] = Nil

    // the possibility of tupleList Nil is considered
    if(tupleList == Nil){
      return null
    }
    if(rowIndex >= tupleList.length){
      null
    }
    else {
      while (rowIndex < tupleList.length && inBlock<blockSize){
        val currentTuple:Tuple = tupleList(rowIndex)
        e(currentTuple) match {
          case true => returnTupleList = returnTupleList :+ currentTuple
          case _ =>
        }
        rowIndex += 1
        inBlock += 1
      }
      returnTupleList.toIndexedSeq
    }



    /*
    if(numBlock >= numBlockMax){
      return null
    }
    else if(numBlock == numBlockMax -1){
      while (inBlock < lastBlock){
        returnTupleList = returnTupleList :+ tupleList( numBlock*blockSize + inBlock)
        inBlock += 1
      }
      numBlock += 1
    }
    else {
      while (inBlock < blockSize){
        returnTupleList = returnTupleList :+ tupleList( numBlock * blockSize + inBlock)
        inBlock += 1
      }
      numBlock += 1
    }
    returnTupleList.toIndexedSeq

     */
  }

  override def close(): Unit = {
    input.close()
  }
}
