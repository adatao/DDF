
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

setGeneric("sql",
           function(x, sql, ...) {
             standardGeneric("sql")
           }
)

setMethod("sql",
          signature("DDFManager", "character"),
          function(x, sql) {
            jdm <- x@jdm
            jdm$sql2txt(sql)
          }
)

setGeneric("sql2ddf",
           function(x, sql, ...) {
             standardGeneric("sql2ddf")
           }
)

setMethod("sql2ddf",
          signature("DDFManager", "character"),
          function(x, sql) {
            jdm <- x@jdm
            new("DDF", jdm$sql2ddf(sql))
          }
)

DDFManager <- function(engine="spark") {
  new("DDFManger", engine=engine)
}