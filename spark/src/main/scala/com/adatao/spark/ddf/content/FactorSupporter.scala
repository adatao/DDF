package com.adatao.spark.ddf.content

import com.adatao.ddf.content.AFactorSupporter
import com.adatao.ddf.content.AFactorSupporter.FactorColumnInfo
import com.adatao.ddf.DDF

/**
 * author: daoduchuan
 */
class FactorSupporter extends AFactorSupporter{

  override def factorize(columnID: Int, ddf: DDF): FactorColumnInfo = {
    new FactorColumnInfo(0);
  }

  override def factorize(columnIDs: Array[Int], ddf: DDF): Array[FactorColumnInfo] ={

    null
  }


}
