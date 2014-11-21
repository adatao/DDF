//package com.adatao.pa.spark.execution

//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.expressions.Row
//import com.adatao.spark.ddf.SparkDDFManager
//import org.apache.spark.graphx.{EdgeTriplet, Edge, Graph}
//import org.apache.spark.mllib.linalg.Vectors
/**
 * author: daoduchuan
 * assuming table with following schema for graph
 * Source         , Dest           , TF-IDF
 * Vertice: String, Vertice: String, EdgeAttribute: double
 */
//class CreateGraph(dataContainerID: String) extends AExecutor[Unit] {

//  override def runImpl(ctx: ExecutionContext): Unit = {
//    val ddf = ctx.sparkThread.getDDFManager.getDDF(dataContainerID)
//    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
//    val sparkContext = ctx.sparkThread.getDDFManager.asInstanceOf[SparkDDFManager].getSparkContext
//    val rddVertices1 = rddRow.map{row => row.getString(0)}
//    val rddVertices2 = rddRow.map{row => row.getString(1)}

//    val Vertices = sparkContext.union(rddVertices1, rddVertices2).map{str => (CreateGraph.hash(str), str)}
//    val edges = rddRow.map {
//      row => Edge(CreateGraph.hash(row.getString(0)), CreateGraph.hash(row.getString(1)), row.getDouble(2))
//    }
//    val graph = Graph(Vertices, edges)

    //Use mapreduceTriplets to generate features vector for each sourcr
//    val rdd = graph.mapReduceTriplets[Seq[(Int, Double)]] (
//      (edge: EdgeTriplet[String, Double]) => {
//        val sourceID = edge.srcId
//        val destAttr = edge.dstAttr
        //use string's hashcode to index the sparse vector
//        Iterator((sourceID, Seq((destAttr.hashCode, edge.attr))))
//      },
//      (x: Seq[(Int, Double)], y: Seq[(Int, Double)]) => x ++ y
//    )
//    val rddFeatures = rdd.map {
//      vertice => (vertice._1, Vectors.sparse(Int.MaxValue * 2 + 1, vertice._2))
//    }
//  }
//}

//object CreateGraph {
  /**
   * Hash a string to a unique Long to to create Graph
   * @param str
   * @return
   */
//  def hash(str: String): Long = {
//    var h = 1125899906842597L
//    var i = 0
//    while(i < str.length) {
//      h = 31 * h + str.charAt(i)
//      i += 1
//    }
//    h
//  }
//}
