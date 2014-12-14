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
import org.slf4j.LoggerFactory

/**
 * author: daoduchuan
 */
class JaccardSimilarity(dataContainerID1: String, dataContainerID2: String, val tfidfThreshold: Double= 0.0,
                          val threshold: Double, val filterDup: Boolean = true)
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
    val rddRow =  JaccardSimilarity.lsh(rddMinHash1, rddMinHash2, threshold)

    val col1 = new Column("caller_1", Schema.ColumnType.LONG)
    val col2 = new Column("caller_2", Schema.ColumnType.LONG)
    //val col3 = new Column("jc_score", Schema.ColumnType.DOUBLE)

    val schema = new Schema(null, Array(col1, col2))

    val newDDF = manager.newDDF(manager, rddRow, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    manager.addDDF(newDDF)
    new DataFrameResult(newDDF)
  }
}

object JaccardSimilarity {
  val LOG = LoggerFactory.getLogger(this.getClass)

  val DEFAULT_NUM_HASHES = "200"
  val DEFAULT_NUM_BANDS = "10"
  val numHashes = System.getProperty("pa.jaccard.numHashes", DEFAULT_NUM_HASHES).toInt
  val numBands = System.getProperty("pa.jaccard.numBands", DEFAULT_NUM_BANDS).toInt

  val minHasher = new MinHasher32(numHashes, numBands)
  def rddRow2rddMinHash(rdd: RDD[Row], threshold: Double): RDD[(Long, MinHashSignature)] = {
    LOG.info(">>> numHashes = " + numHashes)
    LOG.info(">>> numBands = " + numBands)
    val pairRDD: RDD[(Long, Long)] = rdd.map{
      row => {
        if(row.getDouble(2) > threshold) {
          (row.getLong(0), row.getLong(1))
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
  def lsh(rddSignature1: RDD[(Long, MinHashSignature)], rddSignature2: RDD[(Long, MinHashSignature)], threshold: Double) = {
    val buckets1: RDD[(Long, Iterable[Item])] = hashSignature(rddSignature1)
    val buckets2: RDD[(Long, Iterable[Item])] = hashSignature(rddSignature2)

    //group buckets1 and buckets2 by bucket id
    //filter out bucket with empty list in one of the lists
    val rdd: RDD[(Long, (Iterable[Item], Iterable[Item]))] = buckets1.join(buckets2).filter {
      case (bucket, (items1, items2)) => (items1.size > 0) || (items2.size > 0)
    }

    // find similar items in each bucket ussing Jaccard Similarity
    val rddPair: RDD[(Long, (Long, Double))] = rdd.flatMap {
      case (bucket, (items1, items2)) => {
        val iterator1= items1.toIterator
        val iterator2 = items2.toIterator
        val similarItems: ArrayBuffer[(Long, (Long, Double))] = new ArrayBuffer[(Long, (Long, Double))]()
        while(iterator1.hasNext) {
          val item1 = iterator1.next()
          while(iterator2.hasNext) {

            val item2 = iterator2.next()
            val score = minHasher.similarity(item1.signature, item2.signature)
            if(score > threshold) {
              similarItems.append((item1.id, (item2.id, score)))
            }
          }
        }
        similarItems
      }
    }

    //filter out similar pair
    val rddPair2 = rddPair.groupByKey().flatMap {
      case (num1, iter) => {
        val (iter1, iter2) = iter.unzip
        iter1.toList.distinct.map{num2 => Row(num1, num2)}
      }
    }
    rddPair2
  }
  //apply LSH to RDD[(Long, MinHashSignature)]
  //return rdd of bucket and items that belong to each bucket
  def hashSignature(rddSignature: RDD[(Long, MinHashSignature)]): RDD[(Long, Iterable[Item])] = {
    rddSignature.flatMap{
      case (id, sig) => {
        val buckets = minHasher.buckets(sig)
        buckets.map{bucket => (bucket, Item(id, sig))}
      }
    }.groupByKey()
  }
  case class Item(id: Long, signature: MinHashSignature)
}


