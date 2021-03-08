package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var tupleListInput:List[Tuple] = Nil
  val groupByIndex:List[Int] = groupSet.asScala.toList.map(x=>x.asInstanceOf[Int])
  //var groupByIndex:List[Int] = Nil
  var returnTuple:Tuple = null
  var groups:List[(List[RelOperator.Elem],List[Tuple])] = Nil
  var currentGroupsNo:Int = 0
  //var blockIndex:Int = 0


  override def open(): Unit = {
        input.open()

        currentGroupsNo = 0

        var currentTupleList:IndexedSeq[Tuple] = input.next()
        while (currentTupleList!=null){
          tupleListInput = tupleListInput ++ currentTupleList.toList
          currentTupleList = input.next()
        }

        if(tupleListInput != Nil){
          groupByIndex match {
            case Nil => groups = Nil
            case _ => groups =  tupleListInput.groupBy(tuple => groupByIndex.map(i => tuple(i))).toList
          }
        }

  }

  override def next(): Block = {

    if(tupleListInput ==Nil){
      if(currentGroupsNo==0){
        currentGroupsNo +=1
        List(aggCalls.map(aggCall => aggCall.emptyValue).toIndexedSeq).toIndexedSeq
      }
      else null
    }
    else{
      val returnBlock:IndexedSeq[Tuple] = groups match {
        case Nil => {
          if(currentGroupsNo ==0){
            currentGroupsNo +=1
            List(aggCalls.map(aggCall=> {tupleListInput.map(tuple => aggCall.getArgument(tuple)).reduce(aggCall.reduce)}).toIndexedSeq).toIndexedSeq
          }
          else null

          /*
          if(currentGroupsNo == 0){
            currentGroupsNo +=1
            var returnList:List[Tuple] = Nil
            returnList = returnList :+ aggCalls.map(aggCall=> {tupleListInput.map(tuple => aggCall.getArgument(tuple)).reduce(aggCall.reduce)}).toIndexedSeq
            returnList.toIndexedSeq
          }
          else {
            null
          }
          */

        }
        case _ => {
          val blockIndexValuable:List[Int] = (currentGroupsNo until math.min(groups.length, currentGroupsNo +blockSize)).toList
          if(currentGroupsNo < groups.length){
            currentGroupsNo += blockIndexValuable.length

            blockIndexValuable.map(i => {
              (groups(i)._1 ++ aggCalls.map(aggCall=> {groups(i)._2.map(tuple => aggCall.getArgument(tuple)).reduce(aggCall.reduce)})).toIndexedSeq
            }).toIndexedSeq

          }
          else null
        }
      }
      returnBlock
    }
  }

  override def close(): Unit = {
    input.close()
    currentGroupsNo = 0

  }


}
