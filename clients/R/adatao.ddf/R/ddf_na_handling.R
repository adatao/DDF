
#' @export
setGeneric("adatao.dropNA",
           function(x, ...) {
             standardGeneric("adatao.dropNA")
           }
)

setMethod("adatao.dropNA",
          signature("DDF"),
          function(x, axis=0, how=c("any","all"), thresh=0L, columns=NULL, inplace=FALSE) {
            
            how <- match.arg(how)

            if (is.null(columns)) {
              cols <- .jnull('java/util/List')
            } else {
              cols <- .jnew('java/util/ArrayList')
              for (col in columns) {cols$add(col)}
            }
            
            jMissingHandler <- x@jddf$getMissingDataHandler()
            new("DDF", jMissingHandler$dropNA(as.integer(axis), how, .jlong(thresh), cols, inplace))
          }
)


#' @export
setGeneric("adatao.fillNA",
           function(x, ...) {
             standardGeneric("adatao.fillNA")
           }
)

# @param method c('backfill', 'bfill', 'pad', 'ffill')
setMethod("adatao.fillNA",
          signature("DDF"),
          function(x, value, method=NULL, limit=0L, func=NULL, columnsToValues=NULL, columns=NULL, inplace=FALSE) {
            
            method <- match.arg(method)
            
            if (is.null(method)) {
              method <- .jnull('java/lang/String')
            }
            
            if (is.null(func)) {
              func <- .jnull('java/lang/String')
            }
            
            if (is.null(columnsToValues)) {
              colsToVals <- .jnull('java/util/Map')
            } else {
              colsToVals <- .jnew('java/util/HashMap')
              for (key in names(columnsToValues)) {
                colsToVals$put(key, columnsToValues[[key]])
              }
            }
            
            if (is.null(columns)) {
              cols <- .jnull('java/util/List')
            } else {
              cols <- .jnew('java/util/ArrayList')
              for (col in columns) {
                cols$add(col)
              }
            }
            
            jMissingHandler <- x@jddf$getMissingDataHandler()
            
            new("DDF", jMissingHandler$fillNA(value, method, .jlong(limit), func, colsToVals, cols, inplace))
          }
)