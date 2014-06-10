package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.ML.Utils
import com.adatao.basic.ddf.BasicDDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import org.apache.spark.mllib.clustering.KMeansModel
import com.adatao.ddf.ml.IModel

/**
 * author: daoduchuan
 * hardcode for R user meetup demo, to be fixed later
 */
class Deserialize2Model(dataContainerId: String) extends AExecutor[NQLinearRegressionModel] {

  protected override def runImpl(context: ExecutionContext): NQLinearRegressionModel = {
    val manager = context.sparkThread.getDDFManager
    val ddfID = Utils.dcID2DDFID(dataContainerId)
    val ddf = manager.getDDF(ddfID)
    if(ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "not found DDF " + ddfID, null)
    }
//    if(ddf.isInstanceOf[BasicDDF]) {
      ddf.asInstanceOf[BasicDDF].deserialize().asInstanceOf[IModel].getRawModel.asInstanceOf[NQLinearRegressionModel]
//    } else {
//      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error !@#DASDA", null)
//    }
  }
}
