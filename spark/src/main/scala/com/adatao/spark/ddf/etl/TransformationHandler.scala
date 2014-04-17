package com.adatao.spark.ddf.etl

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.rosuda.REngine.REXP
import org.rosuda.REngine.REXPDouble
import org.rosuda.REngine.REXPInteger
import org.rosuda.REngine.REXPList
import org.rosuda.REngine.REXPLogical
import org.rosuda.REngine.REXPString
import org.rosuda.REngine.RList
import org.rosuda.REngine.Rserve.RConnection

import com.adatao.ddf.DDF
import com.adatao.ddf.content.Schema
import com.adatao.ddf.content.Schema.Column
import com.adatao.ddf.etl.{ TransformationHandler ⇒ CoreTransformationHandler }
import com.adatao.ddf.exception.DDFException
import com.adatao.spark.ddf.SparkDDF

class TransformationHandler(mDDF: DDF) extends CoreTransformationHandler(mDDF) {

  override def transformMapReduceNative(mapFuncDef: String, reduceFuncDef: String, mapsideCombine: Boolean = true): DDF = {

    // Prepare data as REXP objects
    val dfrdd = mDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[REXP]).asInstanceOf[RDD[REXP]]

    // 1. map!
    val rMapped = dfrdd.map { partdf ⇒
      try {
        TransformationHandler.preShuffleMapper(partdf, mapFuncDef, reduceFuncDef, mapsideCombine)
      }
      catch {
        case e: Exception ⇒ {

          e match {
            case aExc: DDFException ⇒ throw aExc
            case rserveExc: org.rosuda.REngine.Rserve.RserveException ⇒ {
              throw new DDFException(rserveExc.getMessage, null)
            }
            case _ ⇒ throw new DDFException(e.getMessage, null)
          }
        }
      }
    }

    // 2. extract map key and shuffle!
    val groupped = TransformationHandler.doShuffle(rMapped)

    // 3. reduce!
    val rReduced = groupped.mapPartitions { partdf ⇒
      try {
        TransformationHandler.postShufflePartitionMapper(partdf, reduceFuncDef)
      }
      catch {
        case e: Exception ⇒ {
          e match {
            case aExc: DDFException ⇒ throw aExc
            case rserveExc: org.rosuda.REngine.Rserve.RserveException ⇒ {
              throw new DDFException(rserveExc.getMessage, null)

            }
            case _ ⇒ throw new DDFException(e.getMessage, null)
          }
        }
      }
    }.filter { partdf ⇒
      // mapPartitions after groupByKey may cause some empty partitions,
      // which will result in empty data.frame
      val dflist = partdf.asList()
      dflist.size() > 0 && dflist.at(0).length() > 0
    }

    // convert R-processed DF partitions back to BigR DataFrame
    val (rdd, columnArr) = TransformationHandler.RDataFrameToArrayObject(rReduced)

    // persist because this RDD is expensive to recompute
    rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val newSchema = new Schema(mDDF.getSchemaHandler.newTableName().replace("-", "_"), columnArr.toList);

    new SparkDDF(mDDF.getManager, rdd, classOf[Array[Object]], null, null, newSchema)
  }

  override def transformNativeRserve(transformExpression: String): DDF = {

    val dfrdd = mDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[REXP]).asInstanceOf[RDD[REXP]]

    // process each DF partition in R
    val rMapped = dfrdd.map { partdf ⇒
      try {
        // one connection for each compute job
        val rconn = new RConnection()

        // send the df.partition to R process environment
        val dfvarname = "df.partition"
        rconn.assign(dfvarname, partdf)

        val expr = String.format("%s <- transform(%s, %s)", dfvarname, dfvarname, transformExpression)

        println(">>>>>>>>>>>>.expr=" + expr.toString())

        // compute!
        TransformationHandler.tryEval(rconn, expr, errMsgHeader = "failed to eval transform expression")

        // transfer data to JVM
        val partdfres = rconn.eval(dfvarname)

        // uncomment this to print whole content of the df.partition for debug
        // rconn.voidEval(String.format("print(%s)", dfvarname))
        rconn.close()

        partdfres
      }
      catch {
        case e: DDFException ⇒ {
          throw new DDFException("Unable to perform NativeRserve transformation", e)

        }
      }
    }

    // convert R-processed data partitions back to RDD[Array[Object]]
    val (rdd, columnArr) = TransformationHandler.RDataFrameToArrayObject(rMapped)

    // persist because this RDD is expensive to recompute
    rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val newSchema = new Schema(mDDF.getSchemaHandler.newTableName().replace("-", "_"), columnArr.toList);

    new SparkDDF(mDDF.getManager, rdd, classOf[Array[Object]], null, null, newSchema)
    //mDDF.getSchemaHandler.getSchema().setColumns(columnArr.toList)
    //mDDF.getRepresentationHandler.set(rdd, classOf[RDD[_]], classOf[Array[_]], classOf[Object])
  }

}

object TransformationHandler {

  /**
   * Eval the expr in rconn, if succeeds return null (like rconn.voidEval),
   * if fails raise AdataoException with captured R error message.
   * See: http://rforge.net/Rserve/faq.html#errors
   */  
  def tryEval(rconn: RConnection, expr: String, errMsgHeader: String) {
    rconn.assign(".tmp.", expr)
    val r = rconn.eval("r <- try(eval(parse(text=.tmp.)), silent=TRUE); if (inherits(r, 'try-error')) r else NULL")
    if (r.inherits("try-error")) throw new DDFException(errMsgHeader + ": " + r.asString())
  }

  /**
   * eval the R expr and return all captured output
   */
  def evalCaptureOutput(rconn: RConnection, expr: String): String = {
    rconn.eval("paste(capture.output(print(" + expr + ")), collapse='\\n')").asString()
  }

  /**
   * Convert a RDD of R data.frames into a RDD of Object[]
   */
  def RDataFrameToArrayObject(rdd: RDD[REXP]): (RDD[Array[Object]], Array[Column]) = {

    // get metaInfo of transformed DF
    // and convert R types to Java
    val firstdf = rdd.first()
    val names = firstdf.getAttribute("names").asStrings()
    // detect the output type
    val columns = new Array[Column](firstdf.length)
    for (j ← 0 until firstdf.length()) {
      val bigrType = firstdf.asList().at(j) match {
        case v: REXPDouble ⇒ "double"
        case v: REXPInteger ⇒ "int"
        case v: REXPString ⇒ "string"
        case _ ⇒ throw new DDFException("Only support atomic vectors of type int|double|string!")
      }
      columns(j) = new Column(names(j), bigrType)
    }

    val rddarrobj = rdd.flatMap { partdf ⇒
      val dflist = partdf.asList()
      val partitionSize = (0 until dflist.size()).map(j ⇒ dflist.at(j).length()).reduce { (x, y) ⇒ math.max(x, y) }

      println("partdf.len = " + partdf.length())
      println("partitionSize = " + partitionSize)

      // big allocation!
      val jdata = Array.ofDim[Object](partitionSize, dflist.size())

      // convert R column-oriented AtomicVector to row-oriented Object[]
      // TODO: would be nice to be able to create BigR DataFrame from columnar vectors
      (0 until dflist.size()).foreach { j ⇒
        val rcolvec = dflist.at(j)
        dflist.at(j) match {
          case v: REXPDouble ⇒ {
            val data = rcolvec.asDoubles() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              if (REXPDouble.isNA(data(i)))
                jdata(i)(j) = null
              else
                jdata(i)(j) = data(i).asInstanceOf[Object]
              i += 1
            }
          }
          case v: REXPInteger ⇒ {
            val data = rcolvec.asIntegers() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              if (REXPInteger.isNA(data(i)))
                jdata(i)(j) = null
              else
                jdata(i)(j) = data(i).asInstanceOf[Object]
              i += 1
            }
          }
          case v: REXPString ⇒ {
            val data = rcolvec.asStrings() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              jdata(i)(j) = data(i)
              i += 1
            }
          }
          // TODO: case REXPLogical
        }
      }

      // (0 until partitionSize).foreach { i => println("data(i) = " + util.Arrays.toString(jdata(i))) }

      jdata
    }

    (rddarrobj, columns)
  }

  /**
   * Perform map and mapsideCombine phase
   */
  def preShuffleMapper(partdf: REXP, mapFuncDef: String, reduceFuncDef: String, mapsideCombine: Boolean): REXP = {
    // one connection for each compute job
    val rconn = new RConnection()

    // send the df.partition to R process environment
    rconn.assign("df.partition", partdf)
    rconn.assign("mapside.combine", new REXPLogical(mapsideCombine))

    TransformationHandler.tryEval(rconn, "map.func <- " + mapFuncDef,
      errMsgHeader = "fail to eval map.func definition")
    TransformationHandler.tryEval(rconn, "combine.func <- " + reduceFuncDef,
      errMsgHeader = "fail to eval combine.func definition")

    // pre-amble to define internal functions
    // copied from: https://github.com/adatao/RClient/blob/master/com.adatao.pa/R/mapreduce.R
    // tests: https://github.com/adatao/RClient/blob/mapreduce/com.adatao.pa/inst/tests/test-mapreduce.r#L106
    // should consider some packaging to synchroncize code
    rconn.voidEval(
      """
          |#' Emit keys and values for map/reduce.
          |keyval <- function(key, val) {
          |  if (! is.atomic(key))
          |    stop(paste("keyval: key argument must be an atomic vector: ", paste(key, collapse=" ")))
          |  if (! is.null(dim(key)))
          |    stop(paste("keyval: key argument must be one-dimensional: dim(key) = ",
          |               paste(dim(key), collapse=" ")))
          |  nkey <- length(key)
          |  nval <- if (! is.null(nrow(val))) nrow(val) else length(val)
          |  if (nkey != nval)
          |    stop(sprintf("keyval: key and val arguments must match in length/nrow: %s != %s", nkey, nval))
          |  kv <- list(key=key, val=val);
          |  attr(kv, "adatao-2d-kv-pair") <- T;
          |  kv
          |}
          |
          |#' Emit a single key and value pair for map/reduce.
          |keyval.row <- function(key, val) {
          |  if (! is.null(dim(key)))
          |    stop(paste("keyval: key argument must be a scala value, not n-dimensional: dim(key) = ",
          |               paste(dim(key), collapse=" ")))
          |  if (length(key) != 1)
          |    stop(paste("keyval.row: key argument must be a scalar value: ", paste(key, collapse=" ")))
          |  if (! is.null(dim(val)))
          |    stop(paste("keyval: val argument must be one-: dim(val) = ",
          |               paste(dim(val), collapse=" ")))
          |  kv <- list(key=key, val=val);
          |  attr(kv, "adatao-1d-kv-pair") <- T;
          |  kv
          |}
          |
          |#' does the kv pair have a adatao-defined attr?
          |is.adatao.kv <- function(kv) { (! is.null(attr(kv, "adatao-1d-kv-pair"))) | (! is.null(attr(kv, "adatao-2d-kv-pair"))) }
          |
          |#' should this be splitted?
          |is.adatao.1d.kv <- function(kv) { ! is.null(attr(kv, "adatao-1d-kv-pair")) }
          |
          |do.pre.shuffle <- function(partition, map.func, combine.func, mapside.combine = T, debug = F) {
          |  print("==== map phase begins ...")
          |  kv <- map.func(partition)
          |  if (debug) { print("kv = "); str(kv) }
          |
          |  if (is.adatao.1d.kv(kv)) {
          |    # list of a single keyval object, with the serialized
          |    return(list(keyval.row(kv$key, serialize(kv$val, NULL))))
          |  } else if (!is.adatao.kv(kv)) {
          |    print(paste("skipping non-adatao kv = ", kv))
          |  }
          |
          |  val.bykey <- split(kv$val, f=kv$key)
          |  if (debug) { print("val.bykey ="); str(val.bykey) }
          |  keys <- names(val.bykey)
          |
          |  result <- if (mapside.combine) {
          |    combine.result <- vector('list', length(keys))
          |    for (i in 1:length(val.bykey)) {
          |      kv <- combine.func(keys[[i]], val.bykey[[i]])
          |      if (debug) { print("combined kv = "); str(kv) }
          |      combine.result[[i]] <- keyval.row(kv$key, serialize(kv$val, NULL))
          |    }
          |    # if (debug) print(combine.result)
          |    combine.result
          |  } else {
          |    kvlist.byrow <- vector('list', length(kv$key))
          |    z <- 1
          |    for (i in 1:length(keys)) {
          |      k <- keys[[i]]
          |      vv <- val.bykey[[i]]
          |      if (is.atomic(vv)) {
          |        for (j in 1:length(vv)) {
          |          kvlist.byrow[[z]] <- keyval.row(k, serialize(vv[[j]], NULL))
          |          z <- z + 1
          |        }
          |      } else {
          |        for (j in 1:nrow(vv)) {
          |          kvlist.byrow[[z]] <- keyval.row(k, serialize(vv[j, ], NULL))
          |          z <- z + 1
          |        }
          |      }
          |    }
          |    # if (debug) print(kvlist.byrow)
          |    kvlist.byrow
          |  }
          |  print("==== map phase completed")
          |  # if (debug) { print("kvlist.byrow = "); str(kvlist.byrow) }
          |  result
          |}
        """.stripMargin)

    // map!
    TransformationHandler.tryEval(rconn, "pre.shuffle.result <- do.pre.shuffle(df.partition, map.func, combine.func, mapside.combine, debug=T)",
      errMsgHeader = "fail to apply map.func to data partition")

    // transfer pre-shuffle result into JVM
    val result = rconn.eval("pre.shuffle.result")

    // we will another RConnection because we will now shuffle data
    rconn.close()

    result
  }

  /**
   * By now, whether mapsideCombine is true or false,
   * we both have each partition as a list of list(key=..., val=...)
   */
  def doShuffle(rMapped: RDD[REXP]): RDD[(String, Seq[REXP])] = {
    val groupped = rMapped.flatMap { rexp ⇒
      rexp.asList().iterator.map { kv ⇒
        val kvl = kv.asInstanceOf[REXP].asList()
        val (k, v) = (kvl.at("key").asString(), kvl.at("val"))
        (k, v)
      }
    }.groupByKey()

    // uncomment to debug
    // groupped.collectAsMap().foreach { case(k, vv) => println("k = " + k + ", vv = " + vv.toArray.map(_.toDebugString).mkString(",")) }

    groupped
  }

  /**
   * serialize data to R, perform reduce,
   * then assemble each resulting partition as a data.frame of REXP in Java
   */
  def postShufflePartitionMapper(input: Iterator[(String, Seq[REXP])], reduceFuncDef: String): Iterator[REXP] = {
    val rconn = new RConnection()

    // pre-amble
    // copied from: https://github.com/adatao/RClient/blob/master/com.adatao.pa/R/mapreduce.R
    // tests: https://github.com/adatao/RClient/blob/mapreduce/com.adatao.pa/inst/tests/test-mapreduce.r#L238
    // should consider some packaging to synchronize code
    rconn.voidEval(
      """
          |#' Emit keys and values for map/reduce.
          |keyval <- function(key, val) {
          |  if (! is.atomic(key))
          |    stop(paste("keyval: key argument must be an atomic vector: ", paste(key, collapse=" ")))
          |  if (! is.null(dim(key)))
          |    stop(paste("keyval: key argument must be one-dimensional: dim(key) = ",
          |               paste(dim(key), collapse=" ")))
          |  nkey <- length(key)
          |  nval <- if (! is.null(nrow(val))) nrow(val) else length(val)
          |  if (nkey != nval)
          |    stop(sprintf("keyval: key and val arguments must match in length/nrow: %s != %s", nkey, nval))
          |  kv <- list(key=key, val=val);
          |  attr(kv, "adatao-2d-kv-pair") <- T;
          |  kv
          |}
          |
          |#' Emit a single key and value pair for map/reduce.
          |keyval.row <- function(key, val) {
          |  if (! is.null(dim(key)))
          |    stop(paste("keyval: key argument must be a scala value, not n-dimensional: dim(key) = ",
          |               paste(dim(key), collapse=" ")))
          |  if (length(key) != 1)
          |    stop(paste("keyval.row: key argument must be a scalar value: ", paste(key, collapse=" ")))
          |  if (! is.null(dim(val)))
          |    stop(paste("keyval: val argument must be one-: dim(val) = ",
          |               paste(dim(val), collapse=" ")))
          |  kv <- list(key=key, val=val);
          |  attr(kv, "adatao-1d-kv-pair") <- T;
          |  kv
          |}
          |
          |#' does the kv pair have a adatao-defined attr?
          |is.adatao.kv <- function(kv) { (! is.null(attr(kv, "adatao-1d-kv-pair"))) | (! is.null(attr(kv, "adatao-2d-kv-pair"))) }
          |
          |#' should this be splitted?
          |is.adatao.1d.kv <- function(kv) { ! is.null(attr(kv, "adatao-1d-kv-pair")) }
          |
          |# flatten the reduced kv pair.
          |flatten.kvv <- function(rkv) {
          |  if (length(rkv$val) > 1) {
          |    row <- vector('list', length(rkv$val) + 1)
          |    row[1] <- rkv$key
          |    row[2:(length(rkv$val)+1)] <- rkv$val
          |    names(row) <- c("key", names(rkv$val))
          |    row
          |  } else {
          |    rkv
          |  }
          |}
          |
          |#' bind together list of values from the same keys as rows of a data.frame
          |rbind.vv <- function(vvlist) {
          |  df <- do.call(rbind.data.frame, vvlist)
          |  if (length(vvlist) > 0) {
          |    head <- vvlist[[1]]
          |    if ( is.null(names(head)) ) {
          |      if (length(head) == 1) {
          |        names(df) <- c("val")
          |      } else {
          |        names(df) <- Map(function(x){ paste("val", x, sep="") }, 1:length(head))
          |      }
          |    }
          |  }
          |  df
          |}
          |
          |handle.reduced.kv <- function(rkv) {
          |  if (is.adatao.1d.kv(rkv)) {
          |    row <- flatten.kvv(rkv)
          |    row
          |  } else if (is.adatao.kv(rkv)) {
          |    df <- rkv$val
          |    df$key <- rkv$key
          |    df
          |  } else {
          |    print("skipping not-supported reduce.func output = "); str(rkv)
          |    NULL
          |  }
          |}
        """.stripMargin)

    TransformationHandler.tryEval(rconn, "reduce.func <- " + reduceFuncDef,
      errMsgHeader = "fail to eval reduce.func definition")

    rconn.voidEval("reductions <- list()")
    rconn.voidEval("options(stringsAsFactors = F)")

    // we do this in a loop because each of the seqv could potentially be very large
    input.zipWithIndex.foreach {
      case ((k: String, seqv: Seq[_]), i: Int) ⇒
        println("AHT !!! processing k = " + k + ", len(seqv) = " + seqv.length)

        // send data to R to compute reductions
        rconn.assign("idx", new REXPInteger(i))
        rconn.assign("reduce.key", k)
        rconn.assign("reduce.serialized.vvlist", new REXPList(new RList(seqv)))

        // print to Rserve log
        rconn.voidEval("print(paste('====== processing key = ', reduce.key))")

        TransformationHandler.tryEval(rconn, "reduce.vvlist <- lapply(reduce.serialized.vvlist, unserialize)",
          errMsgHeader = "fail to unserialize shuffled values for key = " + k)
        //println("AHT !!! reduce.vvlist = \n" + RserveUtils.evalCaptureOutput(rconn, "reduce.vvlist"))

        TransformationHandler.tryEval(rconn, "reduce.vv <- rbind.vv(reduce.vvlist)",
          errMsgHeader = "fail to merge (using rbind.vv) shuffled values for key = " + k)
        // println("AHT !!! reduce.vv = \n" + RserveUtils.evalCaptureOutput(rconn, "reduce.vv"))

        // reduce!
        TransformationHandler.tryEval(rconn, "reduced.kv <- reduce.func(reduce.key, reduce.vv)",
          errMsgHeader = "fail to apply reduce func to data partition")
        // println("AHT !!! reduced.kv = \n" + RserveUtils.evalCaptureOutput(rconn, "reduced.kv"))

        // flatten the nested val list if needed
        TransformationHandler.tryEval(rconn, "reduced <- handle.reduced.kv(reduced.kv)",
          errMsgHeader = "malformed reduce.func output, please run mapreduce.local to test your reduce.func")

        // assign reduced item to reductions list
        rconn.voidEval("if (!is.null(reduced)) { reductions[[idx+1]] <- reduced } ")
    }

    // bind the reduced rows together, it contains rows of the resulting BigDataFrame
    TransformationHandler.tryEval(rconn, "reduced.partition <- do.call(rbind.data.frame, reductions)",
      errMsgHeader = "fail to use rbind.data.frame on reductions list, reduce.func cannot be combined as a BigDataFrame")

    // remove weird row names
    rconn.voidEval("rownames(reduced.partition) <- NULL")

    // debug only
    // println("AHT !!! reduced.partition = \n" + RserveUtils.evalCaptureOutput(rconn, "reduced.partition"))

    // transfer reduced data back to JVM
    val result = rconn.eval("reduced.partition")

    // print to Rserve log
    rconn.voidEval("print('==== reduce phase completed')")

    // done R computation for this partition
    rconn.close()

    // wrap it on a Iterator to satisfy mapPartitions
    Iterator.single(result)
  }
}