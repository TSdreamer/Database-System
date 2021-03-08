package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))


  var rowIndex: Int = 0
  val rowCnt: Int = scannable.getRowCount.toInt
  var numPage:Int = 0
  var numPageMax:Int = 0
  val colCnt: Int = table.getRowType.getFieldCount

  override def open(): Unit = {
    rowIndex = 0
  }

  override def next(): Tuple = {
    // when the table is empty
    //println(rowIndex)
    //println("st scan")
    if(rowCnt ==0){
      null
    }
    else{
      if(rowIndex < rowCnt){
        val outputRow:Tuple = scannable match {
          case r: RowStore =>  r.getRow(rowIndex)
          case c: ColumnStore => (0 to (colCnt-1)).toList.map(j => c.getColumn(j)(rowIndex)).toIndexedSeq
          case p:PAXStore => {
            numPage = rowIndex / p.getPAXPage(0)(0).length
            (0 to (colCnt -1)).toList.map(k => p.getPAXPage(numPage)(k)(rowIndex - numPage *p.getPAXPage(0)(0).length)).toIndexedSeq
          }
          case _ => null
        }
        rowIndex +=1
        outputRow
      }
      else {
        //println("end scan")
        null
      }
    }


  }

  override def close(): Unit = {
    rowIndex = 0
    numPage = 0
    numPageMax = 0
  }
}
