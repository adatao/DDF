
#' @exportClass DDFManager
setClass("DDFManager",
         representation(jdm="jobjRef", engine="character"),
         prototype(jdm=NULL, engine="spark")
)

setMethod("initialize",
          signature(.Object="DDFManager"),
          function(.Object, engine) {
            if (is.null(engine))
              engine = "spark"
            .Object@engine = engine
            .Object@jdm = J("com.adatao.ddf.DDFManager")$get(engine)
            .Object
          }
)

#' @export
setGeneric("adatao.sql",
           function(x, sql, ...) {
             standardGeneric("adatao.sql")
           }
)


setMethod("adatao.sql",
          signature("DDFManager", "character"),
          function(x, sql) {
            jdm <- x@jdm
            jdm$sql2txt(sql)
          }
)

#' @export
setGeneric("adatao.sql2ddf",
           function(x, sql, ...) {
             standardGeneric("adatao.sql2ddf")
           }
)

setMethod("adatao.sql2ddf",
          signature("DDFManager", "character"),
          function(x, sql) {
            jdm <- x@jdm
            new("DDF", jdm$sql2ddf(sql))
          }
)

#' @export
setGeneric("shutdown",
           function(x) {
             standardGeneric("shutdown")
           }
)

setMethod("shutdown",
          signature("DDFManager"),
          function(x) {
            jdm <- x@jdm
            jdm$shutdown()
            cat('Bye bye\n')
          }
)


#' @export
DDFManager <- function(engine="spark") {
  new("DDFManager", engine=engine)
}