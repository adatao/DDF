
library(testthat)
library(adatao.ddf)
context("DDF")

load.mtcars <- function() {
  dm <- DDFManager()
  adatao.sql(dm, 'set hive.metastore.warehouse.dir=/tmp')
  adatao.sql(dm, "drop table if exists mtcars")
  adatao.sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  adatao.sql(dm, "LOAD DATA LOCAL INPATH '/home/nhanitvn/adatao/BigR/server/resources/mtcars' INTO TABLE mtcars")
  ddf <- adatao.sql2ddf(dm, "select * from mtcars")
  return(list(dm=dm,ddf=ddf))
}

test_that("basic statistics works", {
  mtc <- load.mtcars()
  dm <- mtc$dm
  ddf <- mtc$ddf
  expect_is(ddf, "DDF")
  expect_identical(colnames(ddf), c("mpg","cyl","disp","hp","drat","wt","qesc","vs","am","gear","carb"))
  expect_equal(ncol(ddf), 11)
  expect_equal(nrow(ddf), 32)
  
  s <- summary(ddf)
  expect_true(abs(s[1,1]-20.091) < 0.001)
  
  ddf2 <- adatao.head(ddf)
  expect_is(ddf2, "DDF")
  
  agg.res <- adatao.aggregate(mpg ~ vs + carb, ddf, FUN=sum)
  expect_identical(agg.res$`sum(mpg)`, c(198.6,145.2,118.5,180.6))
  
  agg.res <- adatao.aggregate(mpg ~ vs + carb, ddf, FUN=mean)
  expect_identical(agg.res$`mean(mpg)`, c(25.34, 25.88, 16.30, 18.92, 19.70, 15.11, 15.00, 18.50))
  
  agg.res <- adatao.aggregate(mpg ~ vs + carb, ddf, FUN=median)
  expect_identical(agg.res$`median(mpg)`, c(22.15, 23.60, 15.80, 17.10, 19.70, 14.30, 15.00, 17.80))
  
  adatao.aggregate(ddf, agg.cols=list(sum(mpg), min(hp)), by=list(vs, am))
  
  
  fn <- adatao.fivenum(ddf)
  expect_equivalent(fn[1,], c(10.400, 4.000, 71.100, 52.000,  2.760,  1.513, 14.500,  0.000,  0.000,  3.000))
  
  
  spl <- adatao.sample(ddf, 10L)
  shutdown(dm)
})


test_that("subsetting works", {
  mtcars <- load.mtcars()
  dm <- mtcars$dm
  ddf <- mtcars$ddf
  
  # pass tests
  ddf2 <- ddf[,c("mpg")]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,"mpg"]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c("mpg", "hp")]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg", "hp"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c(1)]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c(1,2)]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg", "hp"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c("mpg",4)]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg", "hp"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), colnames(ddf))
  expect_equal(nrow(ddf2), 32)
  
  # fail tests
  expect_error(ddf2 <- ddf[,c("mp")])
  
  expect_error(ddf2 <- ddf[,c(-1)])
  
  expect_error(ddf2 <- ddf[,c(12)])
  
  expect_error(ddf2 <- ddf[,c(1.2)])
})

test_that("basic statistics works", {
  dm <- DDFManager()
  adatao.sql(dm, 'set hive.metastore.warehouse.dir=/tmp')
  adatao.sql(dm, "drop table if exists mtcars")
  adatao.sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  adatao.sql(dm, "LOAD DATA LOCAL INPATH '/home/nhanitvn/adatao/BigR/server/resources/mtcars' INTO TABLE mtcars")
  ddf <- adatao.sql2ddf(dm, "select * from mtcars")