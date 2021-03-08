package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import org.w3c.dom.css.Counter

import scala.jdk.CollectionConverters._


class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  var tupleListInput:List[Tuple] = Nil
  val groupByIndex:List[Int] = groupSet.asScala.toList.map(x=>x.asInstanceOf[Int])
  var returnTuple:Tuple = null
  var groups:List[(List[RelOperator.Elem],List[Tuple])] = Nil
  var currentGroupsNo:Int = 0

  override def open(): Unit = {
    // open
    input.open()

    // get the tupleInput List, there will not be the problem
    var tupleInput:Tuple = input.next()
    while (tupleInput!=null){
      tupleListInput = tupleListInput :+ tupleInput
      tupleInput = input.next()
    }
    //println("tupleListInput",tupleListInput)

    // get the groups by the groupSet
    if(tupleListInput != Nil){
     groups =  groupByIndex match {
        case Nil => Nil
        case _ =>  {
          //println(groupByIndex)

          // "groupBy" give us a Hashmap, and I transfer it to a list , and this is not one problem
          tupleListInput.groupBy(tuple => groupByIndex.map(i => tuple(i))).toList
        }
      }
      //println("groups",groups.length)
      //println("curr",currentGroupsNo)
    }

  }
  override def next(): Tuple = {

    if(tupleListInput ==Nil){
      if(currentGroupsNo==0){
        currentGroupsNo +=1
        //println(aggCalls.map(aggCall => aggCall.emptyValue).toIndexedSeq)
        // return the emptyValue if the tupleListInput is empty
        aggCalls.map(aggCall => aggCall.emptyValue).toIndexedSeq
      }
      else {null}
    }
    else{
      val returnTuple:Tuple = groups match {
        case Nil => {
          if(currentGroupsNo == 0){
            currentGroupsNo +=1
            aggCalls.map(aggCall=> {tupleListInput.map(tuple => aggCall.getArgument(tuple)).reduce(aggCall.reduce)}).toIndexedSeq
          }
          else {
            //println("end agg",null)
            null
          }
        }
        case _ => {
          //println("groups:length",groups.length)
          if(currentGroupsNo < groups.length){
            currentGroupsNo +=1
            (groups(currentGroupsNo-1)._1 ++ aggCalls.map(aggCall=> {groups(currentGroupsNo-1)._2.map(tuple => aggCall.getArgument(tuple)).reduce(aggCall.reduce)})).toIndexedSeq
          }
          else null

        }
      }
      //println("agg",returnTuple)
      returnTuple
    }
  }

  override def close(): Unit = {
    input.close()
    currentGroupsNo = 0
  }
}
