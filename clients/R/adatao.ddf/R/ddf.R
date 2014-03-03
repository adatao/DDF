
setClass("DDF",
         representation(jddf="jobjRef"),
         prototype(jddf=NULL)
)

setMethod("initialize",
          signature(.Object="DDF"),
          function(.Object, jddf) {
            if (is.null(jddf) || !inherits(jddf, "jobjRef") || !str_detect(jddf@jclass,"^.*DDF$"))
              stop('DDF must be created from DDFManager')
            .Object@jddf = jddf
            .Object
          }
)