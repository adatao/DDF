library(testthat)
library(adatao.ddf)
context("DDF - missing values handling")

load.airlineWithNA <- function(dm) {
  dm <- DDFManager()
  adatao.sql(dm, 'set hive.metastore.warehouse.dir=/tmp')
  
  adatao.sql(dm, "drop table if exists airline_na")
  
  adatao.sql(dm, paste0("create table airline_na (Year int,Month int,DayofMonth int,"
             , "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
             , "CRSArrTime int,UniqueCarrier string, FlightNum int, "
             , "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
             , "AirTime int, ArrDelay int, DepDelay int, Origin string, "
             , "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
             , "CancellationCode string, Diverted string, CarrierDelay int, "
             , "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
             , "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"))
  
  adatao.sql(dm, "load data local inpath '/home/nhanitvn/adatao/ddf/resources/test/airlineWithNA.csv' into table airline_na")
  
  ddf <- adatao.sql2ddf(dm, "select * from airline_na")
  ddf
}

test_that("basic statistics works", {
  dm <- DDFManager()
  
  ddf <- load.airlineWithNA(dm)
  ddf_no_na <- adatao.dropNA(ddf)
  expect_equal(nrow(ddf_no_na), 9)
  shutdown(dm)
})  