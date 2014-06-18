
#' @export
setGeneric("adatao.dropNA",
           function(x, ...) {
             standardGeneric("adatao.dropNA")
           }
)

setMethod("adatao.dropNA",
          signature("DDF"),
          function(x, size, replace=FALSE, seed=123L) {
            new("DDF", x@jddf$dropNA())
          }
)