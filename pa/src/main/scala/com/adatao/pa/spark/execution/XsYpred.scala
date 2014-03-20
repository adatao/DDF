package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, MetaInfo}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import shark.api.JavaSharkContext
import com.adatao.pa.spark.SharkUtils

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 31/8/13
 * Time: 4:57 PM
 *
 */
class XsYpred(dataContainerID: String, val modelID: String, val xCols: Array[Int])
		extends AExecutor[XsYpredResult] {
	override def runImpl(ctx: ExecutionContext): XsYpredResult = {

		val predictions: RDD[(Array[Double], Int)] = getXsYpred(dataContainerID, modelID, xCols, ctx)

		//box prediction to Object, for putting into DataFrame
		val boxedPredictions: RDD[Array[Object]] = predictions.map{
			case(x, y) => {
				val tmp =  new ArrayBuffer[AnyRef]()
				x.foreach(x => tmp += x.asInstanceOf[Object])
				tmp += y.asInstanceOf[Object]
				tmp.toArray
		}}

		val metaInfos = ctx.sparkThread.getDataManager.get(dataContainerID).getMetaInfo
		val xMetaInfo = (for{
			idx <- xCols
		}yield(new MetaInfo(metaInfos(idx).getHeader, "java.lang.Double"))) :+ new MetaInfo("ypred", "java.lang.Integer")
		val jsc = ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
		val df= new DataFrame(xMetaInfo, JavaRDD.fromRDD(boxedPredictions))

		val sdf = SharkUtils.createSharkDataFrame(df, jsc)
		val uid = ctx.sparkThread.getDataManager.add(sdf)
		new XsYpredResult(uid, sdf.getMetaInfo)
	}
}

class XsYpredResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
