package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.pa.spark.DataManager.DataContainer.ContainerType
import com.adatao.pa.spark.DataManager
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import shark.api.JavaSharkContext
import org.apache.spark.api.java.function.Function

/**
 * Features scaling so that data is centered around 0 with stdev 1.
 */
class TransformScaleStandardScala(dataContainerID: String) extends AExecutor[DataFrameResult] {
	override def runImpl(context: ExecutionContext): DataFrameResult = {
		val dm = context.sparkThread.getDataManager
		Option(dm.get(dataContainerID)) match {
			case Some(dataContainer) ⇒ {
				// first run QuickSummary to get mean and sd of each column
				val summary = new QuickSummary()
						.setDataContainerID(dataContainerID)
						.run(context.sparkThread)
						.asInstanceOf[DataframeStatsResult]

				if (dataContainer.getType == ContainerType.SharkDataFrame) {
					val df = dataContainer.asInstanceOf[DataManager.SharkDataFrame]

					// Compose a transformation query
					val sql = "select " + df.getMetaInfo.zipWithIndex.map { case (col, i) =>
						if (col.getType == "string" || col.hasFactor) {
							col.getHeader
						} else {
							// subtract mean, divide by stdev
							String.format("((%s - %s) / %s) as %s",
								col.getHeader,
								summary.mean(i).asInstanceOf[Object],
								summary.stdev(i).asInstanceOf[Object],
								col.getHeader)
						}
					}.mkString(", ") + " from " + df.getTableName
					LOG.info("Transform sql = {}", sql)

					// XXX: hardcode cache = true, no information in dataframe
					val newdf = df.transform(context.sparkContext.asInstanceOf[JavaSharkContext], sql, true, true)
					val uid = dm.add(newdf)

					new DataFrameResult(uid, newdf.getMetaInfo)

				} else if (dataContainer.getType == ContainerType.DataFrame) {
					val df = dataContainer.asInstanceOf[DataManager.DataFrame]

					val newdf = df.transform(new JavaScaler(df.getMetaInfo.length, summary), true)
					val uid = dm.add(newdf)

					new DataFrameResult(uid, newdf.getMetaInfo)

				} else {
					throw new IllegalArgumentException("not supported: "+dataContainer.getType)
				}
			}
			case None ⇒ throw new IllegalArgumentException("dataContainerID %s doesn't exist in user session".format(dataContainerID))
		}
	}

	// supporting class in order to use DataFrame.transfrom written in Java
	class JavaScaler(val numCol: Int, val summary: DataframeStatsResult) extends Function[Array[Object], Array[Object]] {
		override def call(row: Array[Object]) = {
			(0 until numCol).foreach { i =>
			// XXX: don't use try/catch
				try {
					row(i) = ((row(i).asInstanceOf[Double] - summary.mean(i)) / summary.stdev(i)).asInstanceOf[Object]
				} catch {
					case e: ClassCastException => Unit // do nothing for non-double values
				}
			}
			row
		}
	}

}

class DataFrameResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])
