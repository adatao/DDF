
#' @exportClass DDF
setClass("DDF",
         representation(jddf="jobjRef"),
         prototype(jddf=NULL)
)

setMethod("initialize",
          signature(.Object="DDF"),
          function(.Object, jddf) {
            if (is.null(jddf) || !inherits(jddf, "jobjRef") || !(jddf %instanceof% "com.adatao.ddf.DDF"))
              stop('DDF must be created from DDFManager')
            .Object@jddf = jddf
            .Object
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
            suppressMessages(ret <- as.data.frame(sapply(object@jddf$getSummary(), function(col) {format(c(col$mean(), col$stdev(), col$count(), col$NACount(), col$min(), col$max()), digits=digits)})))
            rownames(ret) <- c("mean", "stdev", "count", "cNA", "min", "max")
            ret
          }
)
