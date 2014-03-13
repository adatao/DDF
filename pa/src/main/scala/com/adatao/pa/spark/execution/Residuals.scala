package adatao.bigr.spark.execution

import adatao.bigr.spark.DataManager.{DataFrame, MetaInfo}
import org.apache.spark.api.java.JavaRDD
import adatao.bigr.spark.SharkUtils
import shark.api.JavaSharkContext

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
		val ytrueypred= new YtrueYpred(dataContainerID, modelID, xCols, yCol)
		val pred= ytrueypred.runImpl(ctx)
		val predContainer= Option(ctx.sparkThread.getDataManager.get(pred.dataContainerID))

		val residuals= predContainer match {
			case Some(df) =>  df match{
				case dx: DataFrame =>	dx.getRDD.rdd.map{arr => arr match {
						case Array(ytrue, ypred) => Array((ytrue.asInstanceOf[Double] - ypred.asInstanceOf[Double]).asInstanceOf[AnyRef])
					}
				}
			}
			case None => throw new IllegalArgumentException("dataContainerID doesn't exist in user session")
		}

		val metaInfo= Array(new MetaInfo("residual", "java.lang.Double"))
		val jsc = ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
		val sdf= SharkUtils.createSharkDataFrame(new DataFrame(metaInfo, JavaRDD.fromRDD(residuals)), jsc)
		val uid = ctx.sparkThread.getDataManager.add(sdf)

		new ResidualsResult(uid, metaInfo)
	}
}

class ResidualsResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
