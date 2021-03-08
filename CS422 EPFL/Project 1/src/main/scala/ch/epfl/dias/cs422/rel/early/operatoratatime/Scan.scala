package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    var output:List[Column] = Nil
    val colCnt: Int = table.getRowType.getFieldCount
    val rowCnt: Int = store.getRowCount.toInt

    if(rowCnt==0){
      IndexedSeq.empty
    }
    else {
      output = store match{
        case r:RowStore => {
          val numRow:Int = r.getRowCount.toInt
          (0 to r.getRow(0).length-1).toList.map(i => (0 to numRow-1).toList.map(r.getRow).map(t => t(i)).toIndexedSeq)
        }
        case c:ColumnStore => {
          (0 until colCnt).toList.map(c.getColumn)
        }
        case p:PAXStore => {
          val rowCnt:Int = p.getRowCount.toInt
          var numPageMax:Int = 0
          var outputCol:Column = null
          var outputTemp:List[Column] = Nil
          numPageMax = (rowCnt-1) / p.getPAXPage(0)(0).length + 1
          for(i <- 0 to p.getPAXPage(0).length-1){
            outputCol = p.getPAXPage(0)(i)
            if(numPageMax > 1){
              for(j <- 1 to numPageMax -1){
                outputCol ++= p.getPAXPage(j)(i)
              }
            }
            else outputCol = outputCol
            outputTemp = outputTemp :+ outputCol
          }
          outputTemp
        }
        case _ => Nil
      }
      output.toIndexedSeq
    }

  }
}
