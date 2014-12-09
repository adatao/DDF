package com.adatao.pa.spark.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.adatao.spark.ddf.SparkDDFManager
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.Vectors
import scala.math.log10
import org.apache.spark.sql.catalyst.expressions.Row
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.execution.GraphTFIDF.CDRVertice
import com.adatao.spark.ddf.content.RepresentationHandler

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

    val rddVertices1: RDD[String] = filteredRDDRow.map{row => {row.getString(srcIdx)}}
    val rddVertices2: RDD[String] = filteredRDDRow.map{row => {row.getString(destIdx)}}


    //create the original graph
    // vertice type of (String, Double) is neccessary for step 3
    val vertices: RDD[(Long, String)] = sparkContext.union(rddVertices1, rddVertices2).map{str => (GraphTFIDF.hash(str), str)}

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

    val graph: Graph[String, Double] = Graph(vertices, edges)
    val partitionedGraph = graph.partitionBy(PartitionStrategy.EdgePartition1D)

    //persist the original graph because it's expensive to create the graph
    partitionedGraph.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    //Step 1
    //calculate CNT column in HH ppt slide
    val groupedEdges: Graph[String, Double] = partitionedGraph.groupEdges((x: Double, y:Double) => x + y)

    //Step 2
    //calculate DN_CNT in HH ppt slide
    //And denominator of tfidf
    // Tuple2(DN_CNT, Denominator of tfidf)
    val dn_cnt: VertexRDD[Tuple2[Double, Double]] = groupedEdges.aggregateMessages(
      (edgeCtx: EdgeContext[String, Double, Tuple2[Double, Double]]) => {
        edgeCtx.sendToSrc((edgeCtx.attr, 0.0))
        edgeCtx.sendToDst((0.0, 1.0))
      }
      ,
      (a: (Double, Double), b: (Double, Double)) => (a._1 + b._1, a._2 + b._2),
      TripletFields.EdgeOnly
    )


    val finalGraph: Graph[CDRVertice, Double] = groupedEdges.outerJoinVertices(dn_cnt) (
      (vertexId: Long, vd: String, tuple: Option[Tuple2[Double, Double]]) => {
        tuple match {
          case Some(tuple2) => CDRVertice(vd, tuple2._1, tuple2._2)
          case None => CDRVertice(vd, 0.0 ,0.0)
        }
      }
    )
    //Step 4 compute total number of calls
    val totalCalls = finalGraph.vertices.map{case (verticeId, CDRVertice(id, dn_cnt, denom)) => dn_cnt}.reduce{case (x , y) => x + y}

    LOG.info(">>>>> totalCalls = " + totalCalls)
    //val idf = log10(totalCalls)

//    val newrdd: RDD[Row] = finalGraph.triplets.map {
//      edge => {
//        val cnt = edge.attr
//        val dn_cnt = edge.srcAttr.dn_cnt
//        val denom_tfidf = edge.dstAttr.denom_tfidf
//        val src = edge.srcAttr.id
//        val dest = edge.dstAttr.id
//        val tf = cnt/dn_cnt
//        val idf = log10(totalCalls/denom_tfidf)
//        val tfidf = tf * idf
//        Row(src, dest, tfidf)
//      }
//    }

    val tfidf_Graph: Graph[String, Double] = finalGraph.mapTriplets(
      (edgeTriplet: EdgeTriplet[CDRVertice, Double]) => {
        val cnt = edgeTriplet.attr
        val dn_cnt = edgeTriplet.srcAttr.dn_cnt
        val denom_tfidf = edgeTriplet.dstAttr.denom_tfidf
        val src = edgeTriplet.srcAttr.id
        val dest = edgeTriplet.dstAttr.id
        val tf = cnt / dn_cnt
        val idf = log10(totalCalls/denom_tfidf)
        val tfidf = tf * idf
        tfidf
      }
    ).mapVertices{case (verticeID, vertice) => vertice.id}
    tfidf_Graph.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val newRDD: RDD[Row] = tfidf_Graph.triplets.map(
      edge => Row(edge.srcAttr, edge.dstAttr, edge.attr)
    )

    val col1 = new Column(src, Schema.ColumnType.STRING)
    val col2 = new Column(dest, Schema.ColumnType.STRING)
    val col3 = new Column("tfidf", Schema.ColumnType.DOUBLE)
    val schema = new Schema(null, Array(col1, col2, col3))

    val newDDF = manager.newDDF(manager, newRDD, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    newDDF.getRepresentationHandler.add(tfidf_Graph, classOf[Graph[_, _]])

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
  case class CDRVertice(id: String, dn_cnt: Double, denom_tfidf: Double)
}
