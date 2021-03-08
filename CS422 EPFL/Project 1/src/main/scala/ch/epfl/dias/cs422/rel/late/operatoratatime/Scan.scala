package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  override def execute(): IndexedSeq[VIDs] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    var output:IndexedSeq[VIDs] = null
    val rowCnt: Int = store.getRowCount.toInt
    if(rowCnt==0){
      IndexedSeq.empty
    }
    else {
      output = (0 until rowCnt).toList.map(i => IndexedSeq(i.toLong)).toIndexedSeq
      output
    }
  }

  def funcBuild(i:Int):VID => Any = (v:VID) => {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    store match {
      case r:RowStore => r.getRow(v.toInt)(i)
      case c:ColumnStore => c.getColumn(i)(v.toInt)
      case p:PAXStore => {
        val numPage:Int = v.toInt / p.getPAXPage(0)(0).length
        p.getPAXPage(numPage)(i)(v.toInt - numPage*p.getPAXPage(0)(0).length)
      }
      case _ => null
    }
  }
  private lazy val evals = new LazyEvaluatorAccess((0 until table.getRowType.getFieldCount).toList.map(i => funcBuild(i)))

  override def evaluators(): LazyEvaluatorAccess = evals
}
