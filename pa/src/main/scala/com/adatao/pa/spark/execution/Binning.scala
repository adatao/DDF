package com.adatao.pa.spark.execution

import scala.annotation.tailrec

import com.adatao.ddf.DDF
import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.pa.spark.Utils
import shark.api.JavaSharkContext
import shark.api.Row

class BinningResult(val dataContainerID: String, val metaInfo: Array[MetaInfo])

/**
 *
 * @param dataContainerID
 * @param col: column name to do binning
 * @param binningType: support 3 binning types "custom", "equalFreq", "equalInterval"
 * @param numBins: number of bins
 * @param breaks: Array of points to specify the intervals, used for binning type "equalFred" and "equalInterval"
 * @param includeLowest: see R cut function
 * @param right:  see R cut function
 * @param decimalPlaces: number of decimal places in range format
 */
class Binning(val dataContainerID: String,
							val col: String,
							val binningType: String,
							val numBins: Int = 0,
							var breaks: Array[Double] = null,
							val includeLowest: Boolean = false,
							val right: Boolean = true,
							val decimalPlaces: Int = 2) extends AExecutor[BinningResult]{

	protected override def runImpl(context: ExecutionContext): BinningResult = {
	  
	  val ddf = context.sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
	  val newddf = ddf.binning(col, binningType, numBins, breaks, includeLowest, right)
	  // binned var are now factors
    //new GetFactor().setDataContainerID(Utils.getDataContainerId(newddf)).setColumnName(col).run(context.sparkThread)
	  new BinningResult(Utils.getDataContainerId(newddf), Utils.generateMetaInfo(newddf.getSchema()))
	}

}
