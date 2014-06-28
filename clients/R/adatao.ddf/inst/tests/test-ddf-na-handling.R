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
  
  adatao.sql(dm, "load data local inpath '../../../resources/test//airlineWithNA.csv' into table airline_na")
  
  ddf <- adatao.sql2ddf(dm, "select * from airline_na")
  ddf
}

test_that("dropNA works", {
  dm <- DDFManager()
  
  ddf <- load.airlineWithNA(dm)
  ddf_no_na_1 <- adatao.dropNA(ddf)
  expect_equal(nrow(ddf_no_na_1), 9)
  
  ddf_no_na_2 <- adatao.dropNA(ddf, 1, "any", 0, NULL)
  expect_equal(ncol(ddf_no_na_2), 22)
  
  shutdown(dm)
})

test_that("fillNA works", {
  dm <- DDFManager()
  
  ddf <- load.airlineWithNA(dm)
  
  ddf1 <- ddf[,c("year", "origin", "securitydelay","lateaircraftdelay")]
  
  ddf_filled_na_1 <- adatao.fillNA(ddf1, "0")
  
  fetched_df <- adatao.head(ddf_filled_na_1, 2)
  expect_equal(fetched_df[2,3], 0)
  expect_equal(fetched_df[2,4], 0)
  
  ddf_filled_na_2<- adatao.fillNA(ddf1, func="mean")
  fetched_df <- adatao.head(ddf_filled_na_2, 2)
  expect_equal(fetched_df[2,4], 31)
  
  cols2Vals <- list(year="2000", securitydelay = "0", lateaircraftdelay = "1")
  ddf_filled_na_3<- adatao.fillNA(ddf1, columnsToValues=cols2Vals)
  fetched_df <- adatao.head(ddf_filled_na_3, 2)
  expect_equal(fetched_df[2,1], 2000)
  expect_equal(fetched_df[2,3], 0)
  expect_equal(fetched_df[2,4], 1)
  
  
  shutdown(dm)
})