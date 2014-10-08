package com.adatao.pa.spark.execution

import com.adatao.spark.ddf.ATestSuite
import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.Utils.DataFrameResult

/**
 * author: daoduchuan
 */
class LoadFileSuite extends ABigRClientTest {
  test("test loadFile") {
    val schema = "Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int"
    val cmd = new LoadFile("../resources/test/airline.csv", schema, ",")
    val result = bigRClient.execute[DataFrameResult](cmd)

  }
}
