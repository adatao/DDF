
library(adatao.ddf)
context("DDFManager")

test_that("sql works", {
  
})

test_that("sql2ddf works", {
  dm <- DDFManager()
  adatao.sql(dm, 'set hive.metastore.warehouse.dir=/tmp')
  adatao.sql(dm, "drop table if exists mtcars")
  adatao.sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  adatao.sql(dm, "LOAD DATA LOCAL INPATH '/home/nhanitvn/adatao/BigR/server/resources/mtcars' INTO TABLE mtcars")
  ddf <- adatao.sql2ddf(dm, "select * from mtcars")
  expect_is(ddf, "DDF")
})