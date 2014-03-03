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
#'library(adatao.ddf)
#'dr <- .jnew("com.adatao.ddf.spark.DDFDriver")
#'dr
#'props <- .jnew("java.util.HashMap")
#'props$put("spark.home", "/home/cuongbk/Projects/AdataoProjects/Sandbox3/incubator-spark")
#'props$get("spark.home")
#'dr$connect("spark://ubuntu:7077", props)
#'serverjar = c(.libPaths()[1], "/adatao.ddf/java/ddf_spark_2.9.3-1.0.jar")
#'serverjar
#'paste(serverjar, collapse = '')
#'serverjar = paste(serverjar, collapse = '')
#'serverjar
#'props$put("RSERVER_JAR", serverjar)
#'props
#'ft <- v$connect("spark://ubuntu:7077", props)
#'ft
#'ddf <- ft$createSchema("abc")
#'ddf
#'
#'@import rJava
#'@import stringr
NULL
