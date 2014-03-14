#' In this example, we perform a regression task on R built-in mtcars dataset.
#' The task here is to predict fuel consumption in miles per gallon.
library(adatao.bigr)
bigr.connect()

#' First, we load data from BigR server's resource directory to a Hive table.
bigr.sql("drop table if exists mtcars")
bigr.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
bigr.sql("LOAD DATA LOCAL INPATH 'resources/mtcars' INTO TABLE mtcars")
df <- bigr.loadHiveTable(table_name="mtcars")

#' We begin with a Linear Regression model. BigR's Linear Regression algorithm produces similar outputs to R lm.
lm_model<- bigr.lm(mpg ~ cyl + drat + hp + gear + carb, df)
summary(lm_model)

#' Cross-validation is used to choose model based on R^2 metric.
lm.cv <- bigr.cv.lm(mpg ~ cyl + drat + hp + gear + carb, df, k = 3, seed=17)
mean(lm.cv)

#' Next, we try randomForest for this task.

k <- 3
kfold_list <- bigr.cv.kfold(data=df,k=k,seed=17)
#do the evaluation
res <- list(k)
for(i in 1:k) {
  train <- kfold_list[[i]]$train
  test <- kfold_list[[i]]$test
  rf <- bigr.randomForest(mpg ~ cyl + drat + hp + gear + carb, train, ntree=100, userSeed=17)
  res[[i]] <- bigr.r2(rf, test)
}

#' randomForest's R^2 is better than Linear Regression.
mean(unlist(res))

bigr.disconnect()