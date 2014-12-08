package com.adatao.spark.ddf.content

import io.ddf.DDF
import io.spark.ddf.content.{RepresentationHandler => SparkRepresentationHandler}
import io.ddf.content.Representation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.graphx.Graph

/**
 * author: daoduchuan
 */
class RepresentationHandler(mddf: DDF) extends SparkRepresentationHandler(mddf) {

  this.removeConvertFunction(SparkRepresentationHandler.RDD_ROW, SparkRepresentationHandler.RDD_REXP)
  this.addConvertFunction(RepresentationHandler.RDD_CACHEDBATCH, SparkRepresentationHandler.RDD_REXP, new RDDCachedBatch2REXP(this.mddf))
}

object RepresentationHandler {
  val RDD_CACHEDBATCH = new Representation(classOf[RDD[_]], classOf[CachedBatch])
  val GRAPH_REPRESENTATION = new Representation(classOf[Graph[_, _]])
}
