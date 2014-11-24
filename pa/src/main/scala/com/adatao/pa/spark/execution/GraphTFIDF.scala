package com.adatao.pa.spark.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.adatao.spark.ddf.SparkDDFManager
import org.apache.spark.graphx.{PartitionStrategy, EdgeTriplet, Edge, Graph, GraphOps}
import org.apache.spark.mllib.linalg.Vectors
import scala.math.log10
import org.apache.spark.sql.catalyst.expressions.Row
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * author: daoduchuan
 *
 *  use graphx to calculate if-idf on HH data
 *  each entry in the table is one call from source number to destination number
 *
 *  @param: srcIdx index of source column
 *  @param: destIdx: index of destination column
 *  retun ddf with field
 *    src, dest, if-idf
 */
class GraphTFIDF(dataContainerID: String, src: String, dest: String, edge: String = "") extends AExecutor[DataFrameResult] {

  override def runImpl(ctx: ExecutionContext): DataFrameResult = {
    val manager = ctx.sparkThread.getDDFManager
    val ddf = manager.getDDF(dataContainerID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, s"not found DDF $dataContainerID", null)
    }
    LOG.info(">>> src =" + src)
    LOG.info(">>> dest = " + dest)
    LOG.info(">>> edge = " + edge)

    val srcIdx = ddf.getColumnIndex(src)
    val destIdx = ddf.getColumnIndex(dest)
    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val filteredRDDRow = if(edge == null || edge.isEmpty()) {
      rddRow.filter {
        row => !(row.isNullAt(srcIdx) || row.isNullAt(destIdx))
      }
    } else {
      val edgeIdx = ddf.getColumnIndex(edge)
      rddRow.filter {
        row => !(row.isNullAt(srcIdx) || row.isNullAt(destIdx) || row.isNullAt(edgeIdx))
      }
    }

    val sparkContext = manager.asInstanceOf[SparkDDFManager].getSparkContext

    val rddVertices1 = filteredRDDRow.map{row => {row.getString(srcIdx)}}
    val rddVertices2 = filteredRDDRow.map{row => {row.getString(destIdx)}}


    //create the original graph
    // vertice type of (String, Double) is neccessary for step 3
    val vertices: RDD[(Long, (String, Double))] = sparkContext.union(rddVertices1, rddVertices2).map{str => (GraphTFIDF.hash(str), (str, 0.0))}

    //if edge column == null, choose 1 as a default value for edge
    val edges = if(edge == null || edge.isEmpty()) {
      filteredRDDRow.map {
        row => Edge(GraphTFIDF.hash(row.getString(srcIdx)), GraphTFIDF.hash(row.getString(destIdx)), 1.0)
      }
    } else {
      val edgeIdx = ddf.getColumnIndex(edge)
      filteredRDDRow.map {
        row => Edge(GraphTFIDF.hash(row.getString(srcIdx)), GraphTFIDF.hash(row.getString(destIdx)), row.getDouble(edgeIdx))
      }
    }

    val graph = Graph(vertices, edges)
    val partitionedGraph = graph.partitionBy(PartitionStrategy.EdgePartition1D)

    //Step 1
    //calculate CNT column in HH ppt slide
    val groupedEdges: Graph[(String, Double), Double] = partitionedGraph.groupEdges((x: Double, y:Double) => x + y)

    //Step 2
    //calculate DN_CNT in HH ppt slide
    val dn_cnt = groupedEdges.mapReduceTriplets(
      (edge: EdgeTriplet[(String, Double), Double]) => {
        val srcId = edge.srcId
        Iterator((edge.srcId, edge.attr))
      },
      (x: Double, y: Double) => x + y
    )

    //Step 3
    //vertice: (string, double) with double is DN_CNT or the total number of count
    //edge: count or CNT column in HH ppt
    val graph2: Graph[(String, Double), Double] = groupedEdges.joinVertices(dn_cnt)(
      (id: Long, attribute: (String, Double), d: Double) => {
        (attribute._1, d)
      }
    )

    //Step 4 compute total number of calls
    val totalCalls = graph2.vertices.map{case (id, tuple) => tuple._2}.reduce{case (x , y) => x + y}
    val idf = log10(totalCalls)
    LOG.info(">>> idf = " + idf)
    val newrdd: RDD[Row] = graph2.triplets.map{
      edge => {
        val cnt = edge.attr
        val dn_cnt = edge.srcAttr._2
        val src = edge.srcAttr._1
        val dest = edge.dstAttr._1
        val tf = cnt/dn_cnt
        val tfidf = tf * idf
        Row(src, dest, tfidf)
      }
    }
    val col1 = new Column(src, Schema.ColumnType.STRING)
    val col2 = new Column(dest, Schema.ColumnType.STRING)
    val col3 = new Column("ifidf", Schema.ColumnType.DOUBLE)
    val schema = new Schema(null, Array(col1, col2, col3))

    val newDDF = manager.newDDF(manager, newrdd, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    manager.addDDF(newDDF)
    new DataFrameResult(newDDF)
  }
}

object GraphTFIDF {
  /**
   * Hash a string to a unique Long to to create Graph
   * @param str
   * @return
   */
  def hash(str: String): Long = {
    var h = 1125899906842597L
    var i = 0
    while(i < str.length) {
      h = 31 * h + str.charAt(i)
      i += 1
    }
    h
  }
}