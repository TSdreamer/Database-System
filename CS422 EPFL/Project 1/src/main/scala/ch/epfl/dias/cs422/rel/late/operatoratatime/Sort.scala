package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var startLine:Int = 0
  var numLine:Int = 0
  //var tupleList:List[Tuple] = Nil
  //var keysInfo:List[(Int,Boolean)] = Nil
  var returnLine:Int = 0
  var returnList:List[Tuple] = Nil
  var keysList:List[RelOperator.Elem] =Nil
  lazy val inputTuples = input.evaluators()
  val inputCols:IndexedSeq[VIDs] = input.execute()
  var output:IndexedSeq[VIDs] = IndexedSeq()

  lazy val inputSorted:IndexedSeq[Tuple] = inputListSorted()

  override def execute(): IndexedSeq[VIDs] = {
    startLine = offset match {
      case null =>0
      case _ => evalLiteral(offset)match {
        case i:Int => i
        case _ => 0
      }
    }
    numLine = fetch match {
      case null =>  inputCols.length - startLine
      case _ => evalLiteral(fetch)match {
        case i:Int => i
        case _ => inputCols.length - startLine
      }
    }

    output = (0 until math.min(numLine,inputCols.length-startLine)).toList.map(i => IndexedSeq(i.longValue())).toIndexedSeq
    output
  }

  def inputListSorted(): IndexedSeq[VIDs] = {

    val inputTupleList:IndexedSeq[Tuple] = inputCols.map(vids => inputTuples(vids))

    // build an implicit object to realise the sorted of tupleList by several possible keys
    implicit object tupleOrdering extends Ordering[Tuple]{

      // define the compareField function, the order of the field is 0 by default
      def compareField(x:Tuple,y:Tuple,order:Int = 0):Int = {
        val field: RelFieldCollation = collation.getFieldCollations.get(order) // the field to be compared
        val index: Int = field.getFieldIndex // the index of field, to give the keyIndex of Tuple

        // public static int compare(Comparable c1,
        //                          Comparable c2,
        //                          int nullComparison)
        //val tuple1 = inputTuples(x)
        //val tuple2 = inputTuples(y)
        val sign: Int = RelFieldCollation.compare(
          x(index).asInstanceOf[Comparable[RelOperator.Elem]],
          y(index).asInstanceOf[Comparable[RelOperator.Elem]],
          field.nullDirection.nullComparison) // we should notice that the nullDirection is necessary

        // define the descending parameter
        val descPara: Int = if(field.getDirection.isDescending) -1 else 1

        val flag:Int = sign match {
          case 0 => {
            // 0 means x = y so we can compare the next field by order + 1
            if(order+1<collation.getFieldCollations.size()){
              compareField(x,y,order+1)
            }else{0}
          }
          case _ => sign*descPara

        }
        flag
      }
      // define the compare function, because this is the function which will be understood
      override def compare(x:Tuple,y:Tuple):Int = {
        compareField(x,y)
      }
    }
    //To sort the tupleList
    val inputList = inputTupleList.sorted
    inputList
  }

  def funcBuild(i:Int):VID => Any = (v:VID) => {
    if(inputCols == IndexedSeq.empty){
      null
    }
    else {
      //inputSorted.map(vids => inputTuples(vids))(v.toInt)(i)
      inputSorted(v.toInt)(i)
    }
  }
  private lazy val evals = new LazyEvaluatorAccess((0 until inputSorted(0).length).toList.map(i => funcBuild(i)))
  override def evaluators(): LazyEvaluatorAccess = evals
}
