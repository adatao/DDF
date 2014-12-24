package com.adatao.pa.spark.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import com.adatao.spark.ddf.{SparkDDF, SparkDDFManager}
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
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

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
    val sparkContext = manager.asInstanceOf[SparkDDFManager].getSparkContext

    val ddf = manager.getDDF(dataContainerID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, s"not found DDF $dataContainerID", null)
    }
    LOG.info(">>> src =" + src)
    LOG.info(">>> dest = " + dest)
    LOG.info(">>> edge = " + edge)

    val srcIdx = ddf.getColumnIndex(src)
    val destIdx = ddf.getColumnIndex(dest)
    val edgeIdx = ddf.getColumnIndex(edge)
    LOG.info("edgeIdx = " + edgeIdx)
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
    val (vertices, edges)  = GraphTFIDF.groupEdge2Graph(filteredRDDRow, srcIdx, destIdx, edgeIdx, sparkContext)
    val groupedEdges: Graph[Long, Double] = Graph(vertices, edges)

    val dn_cnt: VertexRDD[Tuple2[Double, Double]] = groupedEdges.aggregateMessages(
      (edgeCtx: EdgeContext[Long, Double, Tuple2[Double, Double]]) => {
        edgeCtx.sendToSrc((edgeCtx.attr, 0.0))
        edgeCtx.sendToDst((0.0, 1.0))
      }
      ,
      (a: (Double, Double), b: (Double, Double)) => (a._1 + b._1, a._2 + b._2),
      TripletFields.EdgeOnly
    )


    val finalGraph: Graph[CDRVertice, Double] = groupedEdges.outerJoinVertices(dn_cnt) (
      (vertexId: Long, vd: Long, tuple: Option[Tuple2[Double, Double]]) => {
        tuple match {
          case Some(tuple2) => CDRVertice(vd, tuple2._1, tuple2._2)
          case None => CDRVertice(vd, 0.0 ,0.0)
        }
      }
    )
    //Step 4 compute total number of calls
    val totalCalls = finalGraph.vertices.map{case (verticeId, CDRVertice(id, dn_cnt, denom)) => dn_cnt}.reduce{case (x , y) => x + y}

    LOG.info(">>>>> totalCalls = " + totalCalls)


    val tfidf_Graph: Graph[Long, Double] = finalGraph.mapTriplets(
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

    //tfidf_Graph.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    val newRDD: RDD[Row] = tfidf_Graph.triplets.map(
      edge => Row(edge.srcAttr, edge.dstAttr, edge.attr)
    )

    val col1 = new Column(src, Schema.ColumnType.LONG)
    val col2 = new Column(dest, Schema.ColumnType.LONG)
    val col3 = new Column("tfidf", Schema.ColumnType.DOUBLE)
    val schema = new Schema(null, Array(col1, col2, col3))

    val newDDF = manager.newDDF(manager, newRDD, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)

    //cache the resulting ddf and unpersist all graph RDD

    manager.addDDF(newDDF)
    newDDF.asInstanceOf[SparkDDF].cacheTable()

    edges.unpersist()
    groupedEdges.unpersist()
    groupedEdges.edges.unpersist()
    finalGraph.unpersist()
    tfidf_Graph.unpersist()
    dn_cnt.unpersist()

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
  case class CDRVertice(id: Long, dn_cnt: Double, denom_tfidf: Double)

  def groupEdge2Graph(rdd: RDD[Row], srcIdx: Int, destIdx: Int, edgeIdx: Int, sparkContext: SparkContext) = {
    def reduceByKey[K,V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {
      import num._
      collection
        .groupBy(_._1)
        .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
    }

    val pairRDD: RDD[(Long, (Long, Double))] =
      if(edgeIdx >= 0) {
        rdd.map {
          row =>
            if(!row.isNullAt(srcIdx) && !row.isNullAt(destIdx) && !row.isNullAt(edgeIdx)) {
              (row.getLong(srcIdx), (row.getLong(destIdx), row.getDouble(edgeIdx)))
            } else {
              null
            }
        }
      } else {
        rdd.map {
          row =>
            if(!row.isNullAt(srcIdx) && !row.isNullAt(destIdx)) {
              (row.getLong(srcIdx), (row.getLong(destIdx), 1.0))
            } else {
              null
            }
        }
      }.filter(row => row != null)

    val rdd2 = pairRDD.groupByKey().map{
      case (num, iter) => (num, reduceByKey(iter))
    }
    val rdd3: RDD[Row] = rdd2.flatMap {
      case (num, hmap) => {
        for {
          item <- hmap.toList
        } yield(Row(num, item._1, item._2))
      }
    }
    val rddVertices1 = rdd3.map{row => row.getLong(0)}
    val rddVertices2 = rdd3.map{row => row.getLong(1)}
    val vertices: RDD[(Long, Long)] = sparkContext.union(rddVertices1, rddVertices2).map{long => (long, long)}

    val edges: RDD[Edge[Double]] = rdd3.map{row => Edge(row.getLong(0), row.getLong(1), row.getDouble(2))}
    (vertices, edges)
  }
}
