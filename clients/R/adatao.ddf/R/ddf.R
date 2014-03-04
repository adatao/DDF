
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

setGeneric("train", function(x, algoName, params) {
  standardGeneric("train")
})

setMethod("train", signature(x = "DDF", algoName="character"),
  function(x, algoName, params) {
    jmodel = x@jddf$ML$train(algoName, params)
  }
)

setGeneric("lm", function(x, formula, ...) {
  standardGeneric("lm")
}
setMethod("lm", signature(x="DDF", formula, regularized="none", lambda=0, ref.levels=NULL),
  function(x, formula, regularized, lambda, ref.levels) {
  }
)

