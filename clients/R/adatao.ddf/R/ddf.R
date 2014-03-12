
#' @exportClass DDF
setClass("DDF",
         representation(jddf="jobjRef"),
         prototype(jddf=NULL)
)

setMethod("initialize",
          signature(.Object="DDF"),
          function(.Object, jddf) {
            if (is.null(jddf) || !inherits(jddf, "jobjRef") || !(jddf %instanceof% "com.adatao.ddf.DDF"))
              stop('DDF needs a Java object of class "com.adatao.ddf.DDF"')
            .Object@jddf = jddf
            .Object
          }
)

#' @export
setMethod("colnames",
          signature("DDF"),
          function(x) {
            sapply(x@jddf$getColumnNames(), function(obj) {obj$toString()})
          }
)

#' @export
setMethod("nrow",
          signature("DDF"),
          function(x) {
            x@jddf$getNumRows()
          }
)

#' @export
setMethod("ncol",
          signature("DDF"),
          function(x) {
            x@jddf$getNumColumns()
          }
)

#' @export
setMethod("summary",
          signature("DDF"),
          function(object, ..., digits = max(3, getOption("digits")-3)) {
            ret <- as.data.frame(sapply(object@jddf$getSummary(), 
                                        function(col) {c(col$mean(), col$stdev(), col$count(), col$NACount(), col$min(), col$max())}))
            colnames(ret) <- colnames(object)
            rownames(ret) <- c("mean", "stdev", "count", "cNA", "min", "max")
            ret
          }
)

#' Distributed Data Frame subsetting/filtering for extracting values
#' 
#' @section Usage:
#' \describe{
#'  \code{x[i,j,...,drop=TRUE]}
#' }
#' @rdname open-brace
#' @param x the Distributed Data Frame.
#' @param i empty or an expression to filter rows.
#' @param j column names or indices.
#' @param drop coerce the Distributed Data Frame result to an AdataoVector if only one column is specified.
#' @return an Distributed Data Frame or an Distributed Vector.
#' @name [-DDF
#' @export
setMethod("[", signature(x="DDF"),
          function(x, i,j,...,drop=TRUE) {
            .subset(x, i,j,...,drop)
          }  
)

.subset <- function(x, i,j,...,drop) {
  if (missing(j))
    col.names <- colnames(x)
  else
    col.names <- sapply(j, function(col) {
      if (is.numeric(col)) {
        # validate column index
        if (col < 0 || col > ncol(x) || !isInteger(col))
          stop(paste0("Invalid column indices - ",col), call.=F)
        return(colnames(x)[col])
      } else {
        # validate column name
        if (!(col %in% colnames(x)))
          stop(paste0("Invalid column name - ",col), call.=F)
        return(col)
      }
    })
  
  names(col.names) <- NULL
  
  new("DDF", x@jddf$Views$project(.jarray(col.names)))
}


#' Return the First Part of a Distributed Data Object
#' 
#' @export
setGeneric("adatao.head",
           function(x, ...) {
             standardGeneric("adatao.head")
           }
)

#' @details
#' \code{adatao.head} for a Distributed Data Frame returns the first rows of that Distributed Data Frame as an R native data.frame.
#' @param x a Distributed Data Frame
#' @param n a single positive integer, rows for the resulting \code{data.frame}.
#' @return an R native data.frame if \code{x} is a Distributed Data Frame.
#' @rdname adatao.head
#' @rdname adatao.head
setMethod("adatao.head",
          signature("DDF"),
          function(x, n=6L) {
            res <- x@jddf$Views$firstNRows(n)
            # extract values
            df <- as.data.frame(t(sapply(res, function(x){x$split("\t")})), stringsAsFactors=F)
            
            # set types
            coltypes <- .coltypes(x)
            sapply(1:ncol(df), function(idx) {coltype <- coltypes[idx];
                                                    df[,idx] <<- .set.type(df[,idx], coltype)})
            colnames(df) <- colnames(x)
            df
          }
)


#' @export
setGeneric("adatao.sample",
           function(x, ...) {
             standardGeneric("adatao.sample")
           }
)

setMethod("adatao.sample",
          signature("DDF"),
          function(x, size, replace=FALSE, seed=123L) {
            x@jddf$Views$getRandomSample(size, replace, seed)
          }
)

#' @export
setGeneric("adatao.sample2ddf",
           function(x, ...) {
             standardGeneric("adatao.sample2ddf")
           }
)

setMethod("adatao.sample2ddf",
          signature("DDF"),
          function(x, percent, replace=FALSE, seed=123L) {
            x@jddf$Views.getRandomSample(percent, replace, seed)
          }
)

#' Compute Summary Statistics of a Distributed Data Frame's Subsets
#' 
#' Splits a Distributed Data Frame into subsets, computes summary statistics for each, 
#' and returns the result in a convenient form.
#' @rdname adatao.aggregate
adatao.aggregate <- function(x, ...)
  UseMethod("adatao.aggregate")


#' @details \code{adatao.aggregate.formula} is a standard formula interface to \code{adatao.aggregate}.
#' @param formula in format, \code{y ~ x1 + x2} or \code{cbind(y1,y2) ~ x1 + x2} where \code{x1, x2} are group-by variables and \code{y,y1,y2} are variables to aggregate on.
#' @param data a Distributed Data Frame
#' @param FUN the aggregate function, currently support mean, median, var, sum.
#' @return a data frame with columns corresponding to the grouping variables in by followed by aggregated columns from data.
#' @S3method adatao.aggregate formula
#' @export
#' @rdname adatao.aggregate
adatao.aggregate.formula <- function(formula, data, FUN) {
  # parse formula's left hand side expression
  left.vars <- formula[[2]]
  if (length(left.vars) == 1)
    left.vars <- deparse(left.vars)
  else if (identical(deparse(left.vars[[1]]), "cbind")) {
    left.vars <- sapply(left.vars, deparse)[-1]
  } else stop("Unsupported operation on left hand side of the formula!!!")
  
  left.vars <- unique(left.vars)
  
  # parse formula's right hand side expression
  right.vars <- trim(unlist(strsplit(as.character(deparse(formula[[3]]))," \\+ ")))
  vars <- append(left.vars, right.vars)
  vars_idx <- .lookup(vars, colnames(data))
  if (is.character(vars_idx))
    stop(vars_idx)
  
  right.vars <- unique(right.vars)
  
  # variable in left-hand side must not be in righ-hand size
  errors <- NULL
  for(x in left.vars) {
    if (x %in% right.vars) {
      if (is.null(errors)) 
        errors <- paste0(x, " is in both side of formula") 
      else errors <- c(errors, paste0(x, " is in both side of formula"))}
  }
  
  if (!is.null(errors))
    stop(paste(errors, collapse="\n"))
  
  # check if FUN is a valid one
  fname <- deparse(substitute(FUN))
  if (is.na(pmatch(fname,c("sum", "count", "mean","median","variance"))))
    stop("Only support these FUNs: sum, count, mean, median, variance")
  if (identical(fname,"var"))
    fname <- "variance"
  
  # build the query string
  cols_str <- paste(paste0(right.vars, collapse=","), paste0(sapply(left.vars, function(var) {paste0(fname, "(", var, ")")}), collapse=","), sep=",")
  res <- .adatao.aggregate(data, cols_str)
  colnames(res) <- c(right.vars, sapply(left.vars, function(var) {paste0(fname, "(", var, ")")}))
  res
}


setGeneric("adatao.aggregate")

#' @param x a Distributed Data Frame
#' @param agg.cols a list of columns to calculate summary statistics
#' @param by a list of grouping columns 
#' @method adatao.aggregate DDF
#' @export
#' @rdname adatao.aggregate
setMethod("adatao.aggregate",
          signature("DDF"),
          function(x, agg.cols, by) {
            cols_str <- deparse(substitute(agg.cols))
            by_str <- deparse(substitute(by))
            full_str <- paste(substring(by_str, first=6, last=length(by_str)-1), substring(cols_str, first=6, last=length(cols_str)-1), sep=",")
            print(full_str)
            .adatao.aggregate(x, full_str)
          }
)

.adatao.aggregate <- function(x, cols_str) {
  jres <- x@jddf$aggregate(cols_str)
  agg_res <- as.data.frame(sapply(jres$keySet(), function(k) {unlist(sapply(jres$get(k), function(x) {.jarray(x)[[1]]$doubleValue()}))}))
  group_res <- as.data.frame(t(sapply(jres$keySet(), function(x) {x$split(",")})), stringsAsFactors=F)
  cbind(group_res, agg_res)
}


#' Tukey Five-Number Summaries
#' 
#' Return Returns Tukey's five number summary (minimum, lower-hinge, median, upper-hinge, maximum) 
#' for each numeric column of a Distributed Data Frame.
#' @param ddf a Distributed Data Frame.
#' @return a data.frame in which each column is a vector containing the summary information.
#' @export
setGeneric("adatao.fivenum",
           function(ddf) {
             standardGeneric("adatao.fivenum")
           }
)

setMethod("adatao.fivenum",
          signature("DDF"),
          function(ddf) {
            # only numeric columns have those fivenum numbers
            numeric.col.indices <- which(sapply(col.names, function(cn) {ddf@jddf$getColumn(cn)$isNumeric()})==TRUE)
            
            # call java API
            fns <- ddf@jddf$getFiveNumSummary()
            
            # extract values
            ret <- sapply(numeric.col.indices, function(idx) {fn <- fns[[idx]];
                                                c(fn$getMin(), fn$getFirst_quantile(), fn$getMedian(), 
                                                  fn$getThird_quantile(), fn$getMax())})
            
            # set row names
            rownames(ret) <- c("Min.", "1st Qu.", "Median", "3rd Qu.",  "Max.")
            ret
          }
)

.coltypes <- function(ddf) {
  col.names <- colnames(ddf)
  sapply(col.names, function(cn) {ddf@jddf$getColumn(cn)$getType()$toString()})
}

.set.type <- function(v, type) {
  type <- switch(type,
         INT = "integer",
         LONG = "integer",
         DOUBLE = "double",
         FLOAT = "double",
         BOOLEAN = "logical",
         STRING = "character",
         character)
  fn <- paste("as", type, sep=".")
  do.call(fn, list(v))
  
}