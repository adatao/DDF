package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, MetaInfo}
import org.apache.spark.api.java.JavaRDD
import com.adatao.pa.spark.SharkUtils
import shark.api.JavaSharkContext
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.spark.ddf.SparkDDF

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 29/9/13
 * Time: 5:59 PM
 * To change this template use File | Settings | File Templates.
 */
class Residuals(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int)
		extends AExecutor[ResidualsResult]{
	override def runImpl(ctx: ExecutionContext): ResidualsResult = {
//		val ytrueypred= new YtrueYpred(dataContainerID, modelID, xCols, yCol)
//		val pred= ytrueypred.runImpl(ctx)
//		val predContainer= Option(ctx.sparkThread.getDataManager.get(pred.dataContainerID))

		
		val ddfManager = ctx.sparkThread.getDDFManager();
    val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));

    // first, compute RDD[(ytrue, ypred)]
    //all we need is to change it HERE
    //old API
//    val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)
    
    val mymodel: IModel = ddfManager.getModel(modelID)
    val resultDDF = ddf.getMLSupporter().applyModel(mymodel, true, true)
    val predictionsRDD = resultDDF.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]])
    
    val residuals = predictionsRDD .map{arr => arr match {
            case Array(ytrue, ypred) => Array((ytrue.asInstanceOf[Double] - ypred.asInstanceOf[Double]).asInstanceOf[AnyRef])
    }    
    }
//		val residuals= predictionsRDD match {
//			case Some(df) =>  df match{
//				case dx: DataFrame =>	dx.getRDD.rdd.map{arr => arr match {
//						case Array(ytrue, ypred) => Array((ytrue.asInstanceOf[Double] - ypred.asInstanceOf[Double]).asInstanceOf[AnyRef])
//					}
//				}
//			}
//			case None => throw new IllegalArgumentException("dataContainerID doesn't exist in user session")
//		}

    //return dataframe
		val metaInfo= Array(new MetaInfo("residual", "java.lang.Double"))
		val jsc = ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
		val sdf= SharkUtils.createSharkDataFrame(new DataFrame(metaInfo, JavaRDD.fromRDD(residuals)), jsc)
		val uid = ctx.sparkThread.getDataManager.add(sdf)

		new ResidualsResult(uid, metaInfo)
	}
}

class ResidualsResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
