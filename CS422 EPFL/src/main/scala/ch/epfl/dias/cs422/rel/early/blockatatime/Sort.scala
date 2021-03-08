package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  var tupleList:List[Tuple] = List()
  var startLine:Int = 0
  var numLine:Int = 0
  var keysInfo:List[(Int,Boolean)] = Nil
  var returnLine:Int = 0
  var returnList:List[Tuple] = Nil
  var keysList:List[RelOperator.Elem] =Nil
  //var tempLine:Int = 0

  override def open(): Unit = {
    input.open()

    var currentTupleList:IndexedSeq[Tuple] = input.next()

    //var allTupleList:List[Tuple] = Nil
    while(currentTupleList != null){
      tupleList = tupleList ++ currentTupleList.toList
      currentTupleList = input.next()
    }

    // To build the keysInfo
    for(i <- 0 to (collation.getFieldCollations.size()-1)){
      val index:Int = collation.getFieldCollations.get(i).getFieldIndex
      val direction:Boolean = collation.getFieldCollations.get(i).getDirection.isDescending
      keysInfo = keysInfo :+ (index,direction)
    }
    // To get all keys in one List with the form of List[List,List,...], maybe we don't need them for this program
    keysList = tupleList.map(t => {keysInfo.map(i => t(i._1))})

    implicit object tupleOrdering extends Ordering[Tuple]{
      //id: the order of the field to be compared
      def compareField(x:Tuple,y:Tuple,id:Int = 0):Int = {
        val field: RelFieldCollation = collation.getFieldCollations.get(id) // the field to be compared
        val filedNum: Int = field.getFieldIndex
        val sign: Int = RelFieldCollation.compare(
          x(filedNum).asInstanceOf[Comparable[RelOperator.Elem]],
          y(filedNum).asInstanceOf[Comparable[RelOperator.Elem]],
          field.nullDirection.nullComparison)
        val desc: Int = if(field.getDirection.isDescending) -1 else 1

        sign match {
          case 0 => {
            if(id+1<collation.getFieldCollations.size()){
              compareField(x,y,id+1)
            }else{0}
          }
          case _ => sign*desc
        }
      }

      override def compare(x:Tuple,y:Tuple):Int = {
        compareField(x,y)
      }
    }
    //To sort the tupleList
    tupleList = tupleList.sorted

    // To get starLine and numLine, and get the tupleList sorted between them
    // we should notice that the startLine is not the n-th line but the Index of the list

    startLine = offset match {
      case null =>0
      case _ => evalLiteral(offset)match {
        case i:Int => i
        case _ => 0
      }
    }
    returnLine = startLine
    numLine = fetch match {
      case null =>  tupleList.length - startLine
      case _ => evalLiteral(fetch)match {
        case i:Int => i
        case _ => tupleList.length - startLine
      }
    }

  }

  override def next(): Block = {
    if(blockSize == 0 || tupleList.length == 0){
      return null
    }
    val rowsValuable:List[Int] = (returnLine until math.min(tupleList.length,math.min(startLine + numLine,returnLine + blockSize))).toList
    if(returnLine < math.min(tupleList.length,startLine + numLine)){
      returnLine += rowsValuable.length
      rowsValuable.map(tupleList).toIndexedSeq
    }
    else null
  }

  override def close(): Unit = {
    input.close()
    returnLine = 0
  }
}
