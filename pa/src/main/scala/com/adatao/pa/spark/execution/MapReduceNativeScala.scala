package com.adatao.pa.spark.execution

import com.adatao.pa.spark.DataManager.{DataFrame, SharkDataFrame}
import com.adatao.pa.spark.{SharkUtils, RserveUtils}
import org.rosuda.REngine.Rserve.RConnection
import org.rosuda.REngine._
import scala.Some
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import shark.api.JavaSharkContext
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

// @formatter:off

/**
 * This executor performs map/reduce of BigDataFrame into another BigDataFrame
 * 	- serialize whole partitions between R<->JVM using Rserve
 * 	- mapside.combine inside R's memory to reduce serde
 * 	- shuffle key are String, shuffle value are bytes serialize()ed by R itself
 *
 * Author: AHT
 */
class MapReduceNativeScala(val dataContainerID: String,
											val mapFuncDef: String,
											val reduceFuncDef: String,
											val mapsideCombine: Boolean = true) extends AExecutor[DataFrameResult] {
	override def runImpl(context: ExecutionContext): DataFrameResult = {
		val dm = context.sparkThread.getDataManager
		Option(dm.get(dataContainerID)) match {
			case Some(dataContainer) ⇒ {
				val sdf = dataContainer match {
					case df: SharkDataFrame => df
				}

				// Prepare data as REXP objects
				val rddpartdf = RserveUtils.TablePartitionsAsRDataFrame(sdf.getTablePartitionRDD, sdf.getMetaInfo)

				// 1. map!
				val rMapped = rddpartdf.map { partdf =>
					try {
						preShuffleMapper(partdf)
					}
					catch {
						case e: Exception => {
							LOG.error("Exception: ", e)
							e match {
								case aExc: AdataoException => throw aExc
								case rserveExc: org.rosuda.REngine.Rserve.RserveException => {
									rserveExc.getRequestReturnCode match {
										case 2|3 => throw new AdataoException(AdataoExceptionCode.ERR_RSERVE_SYNTAX, rserveExc.getMessage, null)
										case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, rserveExc.getMessage, null)
									}
								}
								case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage, null)
							}
						}
					}
				}

				// 2. extract map key and shuffle!
				val groupped = doShuffle(rMapped)

				// 3. reduce!
				val rReduced = groupped.mapPartitions { partdf =>
					try {
						postShufflePartitionMapper(partdf)
					}
					catch {
						case e: Exception => {
							LOG.error("Exception: ", e)
							e match {
								case aExc: AdataoException => throw aExc
								case rserveExc: org.rosuda.REngine.Rserve.RserveException => {
									rserveExc.getRequestReturnCode match {
										case 2|3 => throw new AdataoException(AdataoExceptionCode.ERR_RSERVE_SYNTAX, rserveExc.getMessage, null)
										case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, rserveExc.getMessage, null)
									}
								}
								case _ => throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage, null)
							}
						}
					}
				}.filter { partdf =>
					// mapPartitions after groupByKey may cause some empty partitions,
					// which will result in empty data.frame
					val dflist = partdf.asList()
					dflist.size() > 0 && dflist.at(0).length() > 0
				}

				// convert R-processed DF partitions back to BigR DataFrame
				val (rdd, meta) = RserveUtils.RDataFrameAsArrayObject(rReduced)

				// convert to SharkDataFrame
				// XXX: this is expensive as hell, should instead directly convert
				// the resulting R-processed DF which is columnar to TablePartition
				// https://app.asana.com/0/5660773543914/9614783227369
				val jsc = context.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
				val bigdf = SharkUtils.createSharkDataFrame(new DataFrame(meta, rdd), jsc)

				// add the rdd to data manager
				val uid = dm.add(bigdf)

				new DataFrameResult(uid, bigdf.getMetaInfo)
			}
			case _ => throw new IllegalArgumentException("dataContainerID not found")
		}
	}

	/**
	 * Perform map and mapsideCombine phase
	 */
	def preShuffleMapper(partdf: REXP): REXP = {
		  // one connection for each compute job
			val rconn = new RConnection()

			// send the df.partition to R process environment
			rconn.assign("df.partition", partdf)
			rconn.assign("mapside.combine", new REXPLogical(mapsideCombine))

			RserveUtils.tryEval(rconn, "map.func <- " + mapFuncDef,
				errMsgHeader="fail to eval map.func definition")
			RserveUtils.tryEval(rconn, "combine.func <- " + reduceFuncDef,
				errMsgHeader="fail to eval combine.func definition")

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
			RserveUtils.tryEval(rconn, "pre.shuffle.result <- do.pre.shuffle(df.partition, map.func, combine.func, mapside.combine, debug=T)",
				errMsgHeader="fail to apply map.func to data partition")

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
		val groupped = rMapped.flatMap { rexp =>
		// println("AHT !!! + rexp = " + rexp.toDebugString)
			rexp.asList().iterator.map { kv =>
			// println("AHT !!! kv = " + kv.toString)
				val kvl = kv.asInstanceOf[REXP].asList()
				val (k, v) = (kvl.at("key").asString(), kvl.at("val"))
				// println("AHT !!! k = "+ k + ", v = " + v)
				(k, v)
			}
		}.groupByKey()

		println("AHT !!! num partitions  = " + groupped.partitions.length)

		// uncomment to debug
		// groupped.collectAsMap().foreach { case(k, vv) => println("k = " + k + ", vv = " + vv.toArray.map(_.toDebugString).mkString(",")) }

		groupped
	}

	/**
	 * serialize data to R, perform reduce,
	 * then assemble each resulting partition as a data.frame of REXP in Java
	 */
	def postShufflePartitionMapper(input: Iterator[(String, Seq[REXP])]): Iterator[REXP] = {
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

			RserveUtils.tryEval(rconn, "reduce.func <- " + reduceFuncDef,
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

				RserveUtils.tryEval(rconn, "reduce.vvlist <- lapply(reduce.serialized.vvlist, unserialize)",
					errMsgHeader="fail to unserialize shuffled values for key = " + k)
				//println("AHT !!! reduce.vvlist = \n" + RserveUtils.evalCaptureOutput(rconn, "reduce.vvlist"))

				RserveUtils.tryEval(rconn, "reduce.vv <- rbind.vv(reduce.vvlist)",
					errMsgHeader="fail to merge (using rbind.vv) shuffled values for key = " + k)
				// println("AHT !!! reduce.vv = \n" + RserveUtils.evalCaptureOutput(rconn, "reduce.vv"))

				// reduce!
				RserveUtils.tryEval(rconn, "reduced.kv <- reduce.func(reduce.key, reduce.vv)",
					errMsgHeader="fail to apply reduce func to data partition")
				// println("AHT !!! reduced.kv = \n" + RserveUtils.evalCaptureOutput(rconn, "reduced.kv"))

				// flatten the nested val list if needed
				RserveUtils.tryEval(rconn, "reduced <- handle.reduced.kv(reduced.kv)",
					errMsgHeader="malformed reduce.func output, please run mapreduce.local to test your reduce.func")

				// assign reduced item to reductions list
				rconn.voidEval("if (!is.null(reduced)) { reductions[[idx+1]] <- reduced } ")
			}

			// debug only
			// println("AHT !!! reductions = \n" + RserveUtils.evalCaptureOutput(rconn, "reductions"))

			// bind the reduced rows together, it contains rows of the resulting BigDataFrame
			RserveUtils.tryEval(rconn, "reduced.partition <- do.call(rbind.data.frame, reductions)",
				errMsgHeader="fail to use rbind.data.frame on reductions list, reduce.func cannot be combined as a BigDataFrame")

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

