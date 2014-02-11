#######################################################
# (c) Copyright 2014, Adatao, Inc. All Rights Reserved.

#' ADATAO's DDF's R Client
#'
#'This package is the R interface portion of ADATAO's DDF API. It allows R
#'users to transparently work with large datasets residing on Hadoop clusters
#'using concepts like AdataoDDF, AdataoVector.
#'
#'Essentially, data from HDFS is loaded to create a AdataoDDF. A AdataoDDF
#'provides operations similar to a normal R data.frame. 
#'
#'@name adatao-package
#'@aliases adatao-package adatao
#'@docType package
#'@author ADATAO
#'
#'Maintainer: ADATAO
#'@examples
#'
#'## AdataoDataFrame
#'#adatao.connect("localhost", 7911)
#'#df <- adatao.LoadTable("resources/input")
#'#colnames(df) <- c("V1","V2","V3")
#'#summary(df)
#'## AdataoVector
#'#v <- df$V1
#'#mean(v)
#'## Linear regression
#'#adatao.lm(V1 ~ V2 + V3, df)
#'#adatao.disconnect()
#'
#'@import rJava
#'@import RJSONIO
#'@import stringr
NULL
