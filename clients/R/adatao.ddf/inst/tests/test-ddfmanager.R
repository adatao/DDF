
library(adatao.ddf)
context("DDFManager")

test_that("sql works", {
  
})

test_that("sql2ddf works", {
  dm <- DDFManager()
  dm$sql('set hive.metastore.warehouse.dir=/tmp')
  dm$sql("drop table if exists mtcars")
  dm$sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  dm$sql("LOAD DATA LOCAL INPATH '/home/nhanitvn/adatao/BigR/server/resources/mtcars' INTO TABLE mtcars")
  ddf <- dm$sql2ddf("select * from mtcars")
  expect_is(ddf, "DDF")
})