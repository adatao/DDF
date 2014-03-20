#' In this example, we do a classification task on income dataset http://archive.ics.uci.edu/ml/datasets/Census+Income.
#' The task here is to determine whether a person makes over 50K a year based on his data.
library(adatao.bigr)
bigr.connect()

#' First, we load data from BigR server's resources directory to a Hive table.
bigr.sql("DROP TABLE IF EXISTS income")
bigr.sql("CREATE TABLE income (age int, workclass string, fnlwgt int, education string, education_num int, marital_status string, occupation string, relationship string, race string, sex string, capital_gain int, capital_loss int, hours_per_week int, native_country string, income string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
bigr.sql("LOAD DATA LOCAL INPATH 'resources/adult.data' INTO TABLE income")

df <- bigr.loadHiveTable(table_name="income")

#' income has two values, "<=50K" and ">50K", we need to transform to 0 and 1 first.
df <- bigr.transform(df, income = ifelse(income == "<=50K",0,1))

#' Then convert string columns to factor ones
df <- bigr.as.factor(df, c("workclass", "education", "marital_status", "occupation", "relationship", "sex", "race", "native_country"))

#' We begin with a Logistic Regression model
glm_model<- bigr.glm(income ~ age + race + sex, df)
summary(glm_model)

#' Cross-validation is used to choose model, here we use AUC metric.
glm.cv <- bigr.cv.glm(income ~ age + race + sex, df, k=3, seed=17)
mean(sapply(glm.cv, function(x) x$auc))

#' BigR provides also randomForest for classification tasks. The following code do cross validation on randomForest

k <- 3
kfold_list <- bigr.cv.kfold(data=df,k=k,seed=17)
#do the evaluation
res <- list(k)
for(i in 1:k) {
  train <- kfold_list[[i]]$train
  test <- kfold_list[[i]]$test
  rf <- bigr.randomForest(income ~ age + sex + race, train, ntree=100, userSeed=17)
  prediction <- bigr.predict(rf, test)
  
  pred <- bigr.metrics(data=prediction, alpha_length=10000)
  res[[i]] <- pred 
}

#' randomForest's AUC is better than Logistic Regression
mean(sapply(res, function(x) x$auc))

bigr.disconnect()