#######################################################
# (c) Copyright 2014, Adatao, Inc. All Rights Reserved.

.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc = libname)
  packageStartupMessage("adatao - for fast, easy and transparent Big Data processing in R")
}
