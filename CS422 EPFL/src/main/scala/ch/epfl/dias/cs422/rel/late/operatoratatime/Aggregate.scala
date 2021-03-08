package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.jdk.CollectionConverters._

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  //var groups = Nil
  lazy val evalsInput = input.evaluators()
  var outputCols:List[VIDs] = Nil
  val inputCols:IndexedSeq[VIDs] = input.execute()
  val keysIndex:List[Int] = groupSet.asScala.toList.map(x=>x.asInstanceOf[Int])

  lazy val groups:List[(List[RelOperator.Elem],IndexedSeq[RelOperator.Tuple])] = groupsBuild(inputCols,keysIndex)

  //lazy val tupleMateria:IndexedSeq[RelOperator.Tuple] = inputCols.map(vids => evalsInput(vids))

  override def execute(): IndexedSeq[VIDs] = {
    if(inputCols == IndexedSeq.empty){
      outputCols = outputCols :+ IndexedSeq(0.longValue())
    }
    else {
      if(groups == Nil){
        outputCols = outputCols :+ IndexedSeq(0.longValue())
      }
      else {
        outputCols = (0 until groups.length).toList.map(i => IndexedSeq(i.longValue()))
      }
    }
    outputCols.toIndexedSeq
  }

  def groupsBuild(inputCols:IndexedSeq[VIDs],keysIndex:List[Int]):List[(List[RelOperator.Elem],IndexedSeq[RelOperator.Tuple])] = {
    var groups:List[(List[RelOperator.Elem],IndexedSeq[RelOperator.Tuple])] = Nil
    if(inputCols == IndexedSeq.empty){
      groups = Nil
    }
    else {
      groups = keysIndex match {
        case Nil => Nil
        case _ =>
          inputCols.map(vids => evalsInput(vids)).groupBy(row => keysIndex.map(i => row(i))).toList
      }
    }
    groups
  }

  def funcBuild(i:Int):VID => Any = (v:VID) => {

    if(inputCols == IndexedSeq.empty){
      aggCalls.map(aggCall => aggCall.emptyValue).toIndexedSeq(i)
    }
    else{
      val returnValue = groups match {
        case Nil => aggCalls.map(aggCall=> {inputCols.map(vids => evalsInput(vids)).map(row => aggCall.getArgument(row)).reduce(aggCall.reduce)}).toIndexedSeq(i)
        case _ =>  groups.map(group =>{ (group._1 ++ aggCalls.map(aggCall=> group._2.map(row => aggCall.getArgument(row)).reduce(aggCall.reduce))).toIndexedSeq})(v.toInt)(i)
      }
      returnValue
    }
  }

  // A List of functions. The i-th function, given an input VID should produce an the i-th element of the tuple referenced by the VID.
  private lazy val evals = new LazyEvaluatorAccess((0 until (groupSet.length + aggCalls.length)).toList.map(i => funcBuild(i)))

  override def evaluators(): LazyEvaluatorAccess = evals
}
