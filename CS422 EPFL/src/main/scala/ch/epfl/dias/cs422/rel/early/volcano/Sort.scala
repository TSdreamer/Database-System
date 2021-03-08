package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var tupleList:List[Tuple] = List()
  var startLine:Int = 0
  var numLine:Int = 0
  var tempLine:Int = 0
  var keysInfo:List[(Int,Boolean)] = Nil
  var returnLine:Int = 0
  var returnList:List[Tuple] = Nil
  var keysList:List[RelOperator.Elem] =Nil

  override def open(): Unit = {
    input.open()

    var currentTuple:Tuple = input.next()
    if(currentTuple == null){
      tupleList = Nil
    }
    while (currentTuple != null){
      tupleList = tupleList :+ currentTuple
      currentTuple = input.next()
    }

    // To build the keysInfo
    for(i <- 0 to (collation.getFieldCollations.size()-1)){
      val index:Int = collation.getFieldCollations.get(i).getFieldIndex
      val direction:Boolean = collation.getFieldCollations.get(i).getDirection.isDescending
      keysInfo = keysInfo :+ (index,direction)
    }
    // To get all keys in one List with the form of List[List,List,...], maybe we don't need them for this program
    keysList = tupleList.map(t => {keysInfo.map(i => t(i._1))})

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

    // To get starLine and numLine, and get the tupleList sorted between them
    // we should notice that the startLine is not the n-th line but the Index of the list

    startLine = offset match {
      case null =>0
      case _ => evalLiteral(offset)match {
        case i:Int => i
        case _ => 0
      }
    }
    tempLine = startLine
    numLine = fetch match {
      case null =>  tupleList.length - startLine
      case _ => evalLiteral(fetch)match {
        case i:Int => i
        case _ => tupleList.length - startLine
      }
    }

  }

  override def next(): Tuple = {


    if (tempLine < math.min(startLine+numLine,tupleList.length)){
      tempLine +=1
      return tupleList(tempLine-1)
    }
    else {
      return null
    }
  }

  override def close(): Unit = {
    input.close()
    tempLine = startLine
  }

}
