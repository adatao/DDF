package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.spark.ddf.{SparkDDFManager, SparkDDF}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.rdd.RDD
import com.twitter.algebird.{MinHasher32, MinHashSignature}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import io.ddf.content.Schema.Column
import io.ddf.content.Schema

/**
 * author: daoduchuan
 */
class JaccardSimilarity(dataContainerID1: String, dataContainerID2: String, val tfidfThreshold: Double= 0.0,
                        val jc_threshhold: Double, val filterDup: Boolean = true)
    extends AExecutor[DataFrameResult] {

  override def runImpl(context: ExecutionContext): DataFrameResult = {
    val manager = context.sparkThread.getDDFManager
    val sparkCtx = manager.asInstanceOf[SparkDDFManager].getSparkContext
    val ddf1 = manager.getDDF(dataContainerID1)
    val ddf2 = manager.getDDF(dataContainerID2)

    val (ddf11, ddf22) = if(filterDup) {
      CosineSimilarity.symmetricDifference2DDFs(ddf1, ddf2, ddf1.getColumnNames.get(0), manager)
    } else {
      (ddf1, ddf2)
    }

    val rdd1 = ddf11.asInstanceOf[SparkDDF].getRDD(classOf[Row])
    val rdd2 = ddf22.asInstanceOf[SparkDDF].getRDD(classOf[Row])

    val rddMinHash1 = JaccardSimilarity.rddRow2rddMinHash(rdd1, tfidfThreshold)
    val rddMinHash2 = JaccardSimilarity.rddRow2rddMinHash(rdd2, tfidfThreshold)

    var switchedOrder = false
    val nrow1 = rddMinHash1.count()
    val nrow2 = rddMinHash2.count()

    val (distMinHash, minHash) = if(nrow1 >= nrow2) {
      (rddMinHash1, rddMinHash2.collect())
    } else {
      switchedOrder = true
      (rddMinHash2, rddMinHash1.collect())
    }

    val broadCastMinHash: Broadcast[Array[(String, MinHashSignature)]] = sparkCtx.broadcast(minHash)

    val result = distMinHash.mapPartitions {
      (iter: Iterator[(String, MinHashSignature)]) => {
        val arr = ArrayBuffer[Row]()
        while(iter.hasNext) {
          val value = iter.next()
          val minHash1 = value._2
          val num1 = value._1
          val minHashSignatures = broadCastMinHash.value
          var i = 0
          while(i < minHashSignatures.size) {
            val minHash2 =  minHashSignatures(i)._2
            val num2 = minHashSignatures(i)._1
            val jc = JaccardSimilarity.minHasher.similarity(minHash1, minHash2)
            if(jc > jc_threshhold) {
              if(!switchedOrder) {
                arr.append(Row(num1, num2, jc))
              } else {
                arr.append(Row(num2, num1, jc))
              }
            }
            i += 1
          }
        }
        arr.toIterator
      }
    }
    val col1 = new Column(ddf1.getColumnNames.get(0), Schema.ColumnType.STRING)
    val col2 = new Column(ddf1.getColumnNames.get(1), Schema.ColumnType.STRING)
    val col3 = new Column("jc_score", Schema.ColumnType.DOUBLE)

    val schema = new Schema(null, Array(col1, col2, col3))

    val newDDF = manager.newDDF(manager, result, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    manager.addDDF(newDDF)
    new DataFrameResult(newDDF)
  }
}

object JaccardSimilarity {
  val minHasher = new MinHasher32(200, 20)
  def rddRow2rddMinHash(rdd: RDD[Row], threshold: Double): RDD[(String, MinHashSignature)] = {

    val pairRDD = rdd.map{
      row => {
        if(row.getDouble(2) > threshold) {
          (row.getString(0), row.getString(1))
        } else {
          null
        }
      }
    }

    pairRDD.filter(row => row != null).groupByKey().map {
      case (number, elements) => {
        val minhash = elements.map {
          value => minHasher.init(value)
        }.reduce((h1, h2) => minHasher.plus(h1, h2))
        (number, minhash)
      }
    }
  }
}


