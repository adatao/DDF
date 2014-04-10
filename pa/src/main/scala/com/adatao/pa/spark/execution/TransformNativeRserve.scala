package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, SharkDataFrame}
import org.rosuda.REngine.Rserve.RConnection
import scala.Some
import com.adatao.pa.spark.{SharkUtils, RserveUtils}
import org.apache.spark.storage.StorageLevel
import shark.api.JavaSharkContext
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.pa.spark.Utils
import com.adatao.pa.spark.DataManager.MetaInfo
/**
 * This executor performs transformation of BigDataFrame using a native R function:
 * 	 - add column using a function(row) {...}
 * 	 - transform existing column
 * 	 - user's expression can access underlying per-partition data.frame named 'df.partition'
 *
 * Author: aht
 */
class TransformNativeRserve(dataContainerID: String, val transformExpression: String) extends AExecutor[DataFrameResult] {
	override def runImpl(context: ExecutionContext): DataFrameResult = {
	  val ddf = context.sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
    val newddf = ddf.Transform.transformNativeRserve(transformExpression);
    // binned var are now factors
    //new GetFactor().setDataContainerID(Utils.getDataContainerId(newddf)).setColumnName(col).run(context.sparkThread)
    new DataFrameResult(Utils.getDataContainerId(newddf), Utils.generateMetaInfo(newddf.getSchema()))
		/*val dm = context.sparkThread.getDataManager
		Option(dm.get(dataContainerID)) match {
			case Some(dataContainer) ⇒ {
				val sdf = dataContainer match {
					case df: SharkDataFrame => df
				}

				val dfrdd = RserveUtils.TablePartitionsAsRDataFrame(sdf.getTablePartitionRDD, sdf.getMetaInfo)

				// process each DF partition in R
				val rMapped = dfrdd.map { partdf =>
					try {
						// one connection for each compute job
						val rconn = new RConnection()

						// send the df.partition to R process environment
						val dfvarname = "df.partition"
						rconn.assign(dfvarname, partdf)

						val expr = String.format("%s <- transform(%s, %s)", dfvarname, dfvarname, transformExpr)
						LOG.info("eval expr = {}", expr)

						// compute!
						RserveUtils.tryEval(rconn, expr, errMsgHeader = "failed to eval transform expression")

						// transfer data to JVM
						val partdfres = rconn.eval(dfvarname)

						// uncomment this to print whole content of the df.partition for debug
						// rconn.voidEval(String.format("print(%s)", dfvarname))
						rconn.close()

						partdfres
					} catch {
						case e: Exception => {
							LOG.error("Exception: ", e)
							e match {
								case aExc: AdataoException => throw aExc
								case rserveExc: org.rosuda.REngine.Rserve.RserveException => {
									rserveExc.getRequestReturnCode match {
										case 2|3 => throw new AdataoException(AdataoExceptionCode.ERR_RSERVE_SYNTAX, rserveExc.getMessage, null)
										case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, rserveExc.getMessage, null)
									}
								}
								case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage, null)
							}
						}
					}
				}

				// convert R-processed DF partitions back to BigR DataFrame
				val (rdd, meta) = RserveUtils.RDataFrameAsArrayObject(rMapped)

				// persist because this RDD is expensive to recompute
				rdd.persist(StorageLevel.MEMORY_AND_DISK)
				val jsc = context.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
				// add the rdd to data manager
				val bigdf = SharkUtils.createSharkDataFrame(new DataFrame(meta, rdd), jsc)
				val uid = dm.add(bigdf)

				new DataFrameResult(uid, bigdf.getMetaInfo)
			}
			case _ => throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, null)
		}*/
	}
	//class DataFrameResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
}
