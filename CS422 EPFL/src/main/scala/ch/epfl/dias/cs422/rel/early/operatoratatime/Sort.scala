package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  var startLine:Int = 0
  var numLine:Int = 0
  var tupleList:List[Tuple] = Nil
  var keysInfo:List[(Int,Boolean)] = Nil
  var returnLine:Int = 0
  var returnList:List[Tuple] = Nil
  var keysList:List[RelOperator.Elem] =Nil

  override def execute(): IndexedSeq[Column] = {
    val inputCols:IndexedSeq[Column] = input.execute()
    val inputCols_t:IndexedSeq[RelOperator.Tuple] = inputCols.transpose



    for( i <- 0 until inputCols_t.length){
      tupleList = tupleList :+ inputCols_t.toList(i)
    }

    // To build the keysInfo
    for(i <- 0 to (collation.getFieldCollations.size()-1)){
      val index:Int = collation.getFieldCollations.get(i).getFieldIndex
      val direction:Boolean = collation.getFieldCollations.get(i).getDirection.isDescending
      keysInfo = keysInfo :+ (index,direction)
    }

    // build an implicit object to realise the sorted of tupleList by several possible keys
    implicit object tupleOrdering extends Ordering[Tuple]{

      // define the compareField function, the order of the field is 0 by default
      def compareField(x:Tuple,y:Tuple,order:Int = 0):Int = {
        val field: RelFieldCollation = collation.getFieldCollations.get(order) // the field to be compared
        val index: Int = field.getFieldIndex // the index of field, to give the keyIndex of Tuple

        // we can get the result of the comparation of x and y by the key
        // RelFieldCollation.compare:
        // public static int compare(Comparable c1,
        //                          Comparable c2,
        //                          int nullComparison)
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
          // x > y => +
          // x < y => -
          // descending -1
          // not descending +1
        }
        flag

      }

      // define the compare function, because this is the function which will be understood
      override def compare(x:Tuple,y:Tuple):Int = {
        compareField(x,y)
      }
    }
    //To sort the tupleList
    tupleList = tupleList.sorted

    // To get starLine and numLine, and get the tupleList between them
    startLine = offset match {
      case null =>0
      case _ => evalLiteral(offset)match {
        case i:Int => i
        case _ => 0
      }
    }
    numLine = fetch match {
      case null =>  tupleList.length - startLine
      case _ => evalLiteral(fetch)match {
        case i:Int => i
        case _ => tupleList.length - startLine
      }
    }

    (startLine until  math.min(startLine+numLine,tupleList.length)).toList.map(i => tupleList(i)).toIndexedSeq.transpose

  }
}
