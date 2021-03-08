package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Block
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  var rowIndex:Int = 0
  val rowCnt:Int = store.getRowCount.toInt
  val colCnt:Int = table.getRowType.getFieldCount
  var numPage:Int = 0
  var numPageMax:Int = 0
  var outputList:List[RelOperator.Tuple] = Nil

  override def open(): Unit = {
    rowIndex = 0
  }

  override def next(): Block = {
    //var blockNum:Int = 0
    if(blockSize == 0 || rowCnt == 0){
      return null
    }
    val rowsValuable:List[Int] = (rowIndex until math.min(rowCnt,rowIndex +blockSize)).toList
    val output:IndexedSeq[RelOperator.Tuple] = store match {
      case r: RowStore => {
        // when all Tuples have been taken , it should return a null
        // outside of the while loop is necessary
        if(rowIndex < rowCnt){
          rowIndex += rowsValuable.length
          rowsValuable.map( row => r.getRow(row)).toIndexedSeq

        }
        else {
          null
        }
      }
      case c: ColumnStore => {
        // when all Tuples have been taken , it should return a null
        // outside of the while loop is necessary
        if(rowIndex < rowCnt){
          rowIndex += rowsValuable.length
          (0 to colCnt-1).toList.map(c.getColumn).map(col=>rowsValuable.map(col).toIndexedSeq).toIndexedSeq.transpose

        }
        else null
      }


      case p: PAXStore =>{
        // check if the table is empty
        // get the number of the minipage
        if(rowIndex < rowCnt) {
          rowIndex += rowsValuable.length
          rowsValuable.map(i => {
            numPage = i / p.getPAXPage(0)(0).length
            (0 to (colCnt - 1)).toList.map(col => p.getPAXPage(numPage)(col)(i - numPage*p.getPAXPage(0)(0).length)).toIndexedSeq

          }).toIndexedSeq
        }
        else null

      }
      case _ => null
    }
    return output
  }

  override def close(): Unit = {
    rowIndex = 0
    numPage = 0
    numPageMax = 0
  }
}
