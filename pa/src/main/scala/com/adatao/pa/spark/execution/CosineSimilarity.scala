package com.adatao.pa.spark.execution

import com.adatao.spark.ddf.content.RepresentationHandler
import org.apache.spark.graphx.{EdgeContext, VertexRDD, Graph}
import com.twitter.algebird.{BloomFilterMonoid, BF}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.{norm, SparseVector}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.Row
import com.adatao.pa.spark.Utils.DataFrameResult
import io.ddf.content.Schema.Column
import io.ddf.content.Schema
import com.adatao.spark.ddf.SparkDDFManager

/**
 * author: daoduchuan
 */
class CosineSimilarity(dataContainerID1: String, dataContainerID2: String, val threshold: Double) extends AExecutor[DataFrameResult] {

  override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val sparkCtx: SparkContext = manager.asInstanceOf[SparkDDFManager].getSparkContext
    val ddf1 = manager.getDDF(dataContainerID1)
    val ddf2 = manager.getDDF(dataContainerID2)
    val graph1: Graph[String, Double] = ddf1.getRepresentationHandler.get(RepresentationHandler.GRAPH_REPRESENTATION.
      getTypeSpecsString).asInstanceOf[Graph[String, Double]]
    val graph2: Graph[String, Double] = ddf2.getRepresentationHandler.get(RepresentationHandler.GRAPH_REPRESENTATION.
      getTypeSpecsString).asInstanceOf[Graph[String, Double]]

    val vertices1 = graph1.vertices
    val vertices2 = graph2.vertices

    val vertice1BF = CosineSimilarity.createBloomFilter(vertices1)
    val vertice2BF = CosineSimilarity.createBloomFilter(vertices2)
    LOG.info("vertice1BF.size = " + vertice1BF.size.estimate)
    LOG.info("vertice2BF.size = " + vertice2BF.size.estimate)
    LOG.info("vertices1.size = " + vertices1.count())
    LOG.info("vertices2.size = " + vertices2.count())

    val broadcastVBF1 = sparkCtx.broadcast(vertice1BF)
    val broadcastVBF2 = sparkCtx.broadcast(vertice2BF)

    val diff21 = graph2.subgraph(vpred = ((v, d) => !broadcastVBF1.value.contains(d).isTrue))
    val diff12 = graph1.subgraph(vpred = ((v, d) => !broadcastVBF2.value.contains(d).isTrue))

//    val diff21: VertexRDD[String] = vertices1.diff(vertices2)
//
//    val diff12: VertexRDD[String] = vertices2.diff(vertices1)

    LOG.info("diff21.size = " + diff21.vertices.count())
    LOG.info("diff12.size = " + diff12.vertices.count())
    //need to filter graph2 with only src vertex from diff21
    val bloomFilter2: BF = CosineSimilarity.createBloomFilter(diff21.vertices)
    val bloomFilter1: BF = CosineSimilarity.createBloomFilter(diff12.vertices)

    val broadcastBF1: Broadcast[BF] = sparkCtx.broadcast(bloomFilter1)
    val broadcastBF2: Broadcast[BF] = sparkCtx.broadcast(bloomFilter2)

    println("bloomFilter2.contains(HCM) = " + bloomFilter2.contains("HCM").isTrue)
    println("bloomFilter1.contains(SNA) = " + bloomFilter1.contains("SNA").isTrue)
    println("bloomFilter1.size = " + bloomFilter1.size.estimate)
    println("bloomFilter2.size = " + bloomFilter2.size.estimate)
//    val filteredGraph2 = graph2.subgraph(vpred = ((v, d) => broadcastBF2.value.contains(d).isTrue))
//    val filteredGraph1 = graph1.subgraph(vpred = ((v, d) => broadcastBF1.value.contains(d).isTrue))
    val filteredGraph2 = graph2.subgraph(epred =
      (edge =>
          {
            val isTrue = broadcastBF1.value.contains(edge.srcAttr).isTrue
            println(s"edge.srcAttr = ${edge.srcAttr}, isTrue=$isTrue")
            isTrue
          }
      ))
    val filteredGraph1 = graph1.subgraph(epred =
      (edge =>
      {
        val isTrue = broadcastBF2.value.contains(edge.srcAttr).isTrue
        println(s"edge.srcAttr = ${edge.srcAttr}, isTrue=$isTrue")
        isTrue
      }
        ))
    val count1 = filteredGraph1.vertices.count()
    val count2 = filteredGraph2.vertices.count()
    println("filteredGraph1.vertices.count() = " + count1)
    println("filteredGraph2.vertices.count() = " + count2)
    val arr1 = filteredGraph1.triplets.collect()
    val arr2 = filteredGraph2.triplets.collect()
    arr1.map(edge => println(s">>>edge1 = ${edge.srcAttr} -> ${edge.dstAttr} : ${edge.attr}"))
    arr2.map(edge => println(s">>>edge2 = ${edge.srcAttr} -> ${edge.dstAttr} : ${edge.attr}"))
    val matrix1 = CosineSimilarity.tfIDFGraph2Matrix(filteredGraph1)
    val matrix2 = CosineSimilarity.tfIDFGraph2Matrix(filteredGraph2)
    val localMatrix = matrix2.collect()

    val broadcastMatrix: Broadcast[Array[(String, SparseVector[Double])]] = sparkCtx.broadcast(localMatrix)
    val result: RDD[Row] = matrix1.mapPartitions {
      (iter: Iterator[(String, SparseVector[Double])]) => {
        //val arr: ArrayBuffer[Tuple3[String, String, Double]] =  ArrayBuffer[Tuple3[String, String, Double]]()
        val arr: ArrayBuffer[Row] = ArrayBuffer[Row]()
        while(iter.hasNext) {
          val (num1, vector1) = iter.next()
          val mat: Array[(String, SparseVector[Double])] = broadcastMatrix.value
          var i = 0
          while(i < mat.size) {
            val vector2 = mat(i)._2
            val num2 = mat(i)._1
            val mul: Double = vector2.dot(vector1)

            val cosine = mul / (CosineSimilarity.normVector(vector1) * CosineSimilarity.normVector(vector2))
            println(s">>>> vector2 = ${vector2.toString()}")
            println(s">>>> vector1 = ${vector1.toString()}")
            println(s">>> mul = $mul")
            println(s">>> num1 = $num1 , num2=$num2")
            println(">>>> cosine = " + cosine)
            //only append to result if cosine > threshold
            if(cosine > threshold) {
              arr append Row(num1, num2, cosine)
            }
            i += 1
          }
        }
        arr.toIterator
      }
    }
    val col1 = new Column("number1", Schema.ColumnType.STRING)
    val col2 = new Column("number2", Schema.ColumnType.STRING)
    val col3 = new Column("score", Schema.ColumnType.DOUBLE)

    val schema = new Schema(null, Array(col1, col2, col3))

    val newDDF = manager.newDDF(manager, result, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)

    manager.addDDF(newDDF)
    new DataFrameResult(newDDF)
  }
}

object CosineSimilarity {

  //val numHashes =
  def createBloomFilter(vertexRDD: VertexRDD[String]): BF = {
    //http://hur.st/bloomfilter?n=1000000&p=0.2
    val n = vertexRDD.count()
    val width = n * 20
    val numHashes = scala.math.round(scala.math.log(2) * width / n).toInt
    val bloomFilterMonoid = BloomFilterMonoid(numHashes, width.toInt, 17)
    val bfRDD: RDD[BF] = vertexRDD.mapPartitions {
      (iterator: Iterator[(Long, String)]) => {
        var bf = bloomFilterMonoid.zero
        while(iterator.hasNext) {

          val (id, item) = iterator.next()
          bf = bf + item
        }
        Iterator(bf)
      }
    }
    bfRDD.reduce{case (bf1, bf2) => bf1 ++ bf2}
  }

  def tfIDFGraph2Matrix(graph: Graph[String, Double]): RDD[(String, SparseVector[Double])] = {
    val vertices: VertexRDD[Tuple2[String, Seq[Tuple2[Int, Double]]]] = graph.aggregateMessages (
      (edgeCtx: EdgeContext[String, Double, Tuple2[String, Seq[Tuple2[Int, Double]]]]) => {
        val tfidf = edgeCtx.attr
        val id = scala.math.abs(edgeCtx.dstAttr.hashCode)
        edgeCtx.sendToSrc(edgeCtx.srcAttr, Seq((id, tfidf)))
      },
      (seq1: Tuple2[String, Seq[Tuple2[Int, Double]]], seq2: Tuple2[String, Seq[Tuple2[Int, Double]]]) => Tuple2(seq1._1, seq1._2 ++ seq2._2)
    )

    val parseMatrix = vertices.map {
      case (id, (num, elements)) => {
        val (indices, values) = elements.sortBy(_._1).unzip
        Tuple2(num, new SparseVector[Double](indices.toArray, values.toArray, Int.MaxValue))
      }
    }
    parseMatrix
  }

  def normVector(vector: SparseVector[Double]): Double = {
    val data = vector.data

    var i = 0
    var norm = 0.0
    while( i < data.size) {
      val value = data(i)
      norm += value * value
      i += 1
    }
    scala.math.sqrt(norm)
  }
}
