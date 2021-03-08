package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val inputCols:IndexedSeq[Column] = input.execute()
    val inputCols_t:IndexedSeq[RelOperator.Tuple] = inputCols.transpose
    val keysIndex:List[Int] = groupSet.asScala.toList.map(x=>x.asInstanceOf[Int])
    //var keysGroup:List[RelOperator.Elem] = Nil
    var groups:List[(List[RelOperator.Elem],IndexedSeq[RelOperator.Tuple])] = Nil
    var outputCols:List[Column] = Nil
    /*
    for(i <- 0 to groupSet.length()-1){
      if (groupSet.get(i)){keysIndex = keysIndex :+ i}
    }
    */

    if(inputCols_t != Nil){
      keysIndex match {
        case Nil => Nil
        case _ =>  groups = inputCols_t.groupBy(row => keysIndex.map(i => row(i))).toList
      }
    }

    if(inputCols_t == IndexedSeq.empty){
      outputCols = outputCols :+ aggCalls.map(aggCall => aggCall.emptyValue).toIndexedSeq
      outputCols.toIndexedSeq.transpose
    }
    else {
      if(groups == Nil){
        outputCols = outputCols :+ aggCalls.map(aggCall=> {inputCols_t.map(row => aggCall.getArgument(row)).reduce(aggCall.reduce)}).toIndexedSeq
        outputCols.toIndexedSeq.transpose
      }
      else {
        outputCols = groups.map(group =>{ (group._1 ++ aggCalls.map(aggCall=> group._2.map(row => aggCall.getArgument(row)).reduce(aggCall.reduce))).toIndexedSeq})
        outputCols.toIndexedSeq.transpose
      }
    }







  }
}
