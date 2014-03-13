package com.adatao.pa.spark

import org.apache.spark.rdd.RDD
import shark.memstore2.TablePartition
import com.adatao.pa.spark.DataManager.MetaInfo
import org.rosuda.REngine._
import scala.collection.mutable
import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import com.adatao.ML.TCanLog
import org.rosuda.REngine.Rserve.RConnection
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * @author aht
 */
object RserveUtils extends TCanLog {
	/**
	 * Eval the expr in rconn, if succeeds return null (like rconn.voidEval),
	 * if fails raise AdataoException with captured R error message.
	 * See: http://rforge.net/Rserve/faq.html#errors
	 */
	def tryEval(rconn: RConnection, expr: String, errMsgHeader: String) {
		rconn.assign(".tmp.", expr)
		val r = rconn.eval("r <- try(eval(parse(text=.tmp.)), silent=TRUE); if (inherits(r, 'try-error')) r else NULL")
		if (r.inherits("try-error")) throw new AdataoException(AdataoExceptionCode.ERR_RSERVE_EVAL, errMsgHeader + ": " + r.asString(), null)
	}

	/**
	 * eval the R expr and return all captured output
	 */
	def evalCaptureOutput(rconn: RConnection, expr: String): String = {
		rconn.eval("paste(capture.output(print("+expr+")), collapse='\\n')").asString()
	}

	/**
	 * Transform a bigr data frame into a rdd of native renjin dataframes per partition (as Java objects)
	 * note that a R data frame is just a list of column vectors with additional attributes
	 */
	def TablePartitionsAsRDataFrame(rdd: RDD[TablePartition], metaInfo: Array[MetaInfo]): RDD[REXP] = {
		rdd.filter { tp => tp.iterator.columnIterators.length > 0 }.map { tp =>
			println("AHT !!! tp.numRows = "+tp.numRows)
			println("AHT !!! columnIterators.length = "+tp.iterator.columnIterators.length)

			// each TablePartition should not have more than MAX_INT rows,
			// ArrayBuffer doesn't allow more than that anyway
			val numRows = tp.numRows.asInstanceOf[Int]

			val columns = metaInfo.zipWithIndex.map { case (colMeta, colNo) =>
				LOG.info("processing column: {}, index = {}", colMeta, colNo)
				val iter = tp.iterator.columnIterators(colNo)
				val rvec = colMeta.getType match {
					case "int" | "java.lang.Integer" => {
						val builder = new mutable.ArrayBuilder.ofInt
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								builder += iter.current.asInstanceOf[IntWritable].get
							else
								builder += REXPInteger.NA
							i += 1
						}
						new REXPInteger(builder.result())
					}
					case "double" | "java.lang.Double" => {
						val builder = new mutable.ArrayBuilder.ofDouble
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								builder += iter.current.asInstanceOf[DoubleWritable].get
							else
								builder += REXPDouble.NA
							i += 1
						}
						new REXPDouble(builder.result())
					}
					case "string" | "java.lang.String" => {
						val buffer = new mutable.ArrayBuffer[String](numRows)
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								buffer += iter.current.asInstanceOf[Text].toString
							else
								buffer += null
							i += 1
						}
						new REXPString(buffer.toArray)
					}
					// TODO: REXPLogical
				}
				rvec.asInstanceOf[REXP]
			}

			// named list of columns with colnames
			val dflist = new RList(columns, metaInfo.map { m => m.getHeader })

			// this is the per-partition Renjin data.frame
			REXP.createDataFrame(dflist)
		}
	}

	/**
	 * Convert a RDD of R data.frames into a BigR DataFrame (RDD of Object[]).
	 */
	def RDataFrameAsArrayObject(rdd: RDD[REXP]): (RDD[Array[Object]], Array[MetaInfo]) = {

		// get metaInfo of transformed DF
		// and convert R types to Java
		val firstdf = rdd.first()
		val names = firstdf.getAttribute("names").asStrings()
		// detect the output type
		val meta = new Array[MetaInfo](firstdf.length)
		for (j <- 0 until firstdf.length()) {
			val bigrType = firstdf.asList().at(j) match {
				case v: REXPDouble => "double"
				case v: REXPInteger => "int"
				case v: REXPString => "string"
				case _ => throw new AdataoException(AdataoExceptionCode.ERR_RSERVE_SYNTAX, "BigR only support atomic vectors of type int|double|string", null)
			}
			meta(j) = new MetaInfo(names(j), bigrType, j)
		}

		val rddarrobj = rdd.flatMap { partdf =>
			val dflist = partdf.asList()
			val partitionSize = (0 until dflist.size()).map(j => dflist.at(j).length()).reduce{ (x,y) => math.max(x,y) }

			println("AHT !!! partdf.len = " + partdf.length())
			println("AHT !!! partitionSize = " + partitionSize)

			// big allocation!
			val jdata = Array.ofDim[Object](partitionSize, dflist.size())

			// convert R column-oriented AtomicVector to row-oriented Object[]
			// TODO: would be nice to be able to create BigR DataFrame from columnar vectors
			(0 until dflist.size()).foreach { j =>
				val rcolvec = dflist.at(j)
				dflist.at(j) match {
					case v: REXPDouble => {
						val data = rcolvec.asDoubles()  // no allocation
						var i = 0  // row idx
						while (i < partitionSize) {
							if (REXPDouble.isNA(data(i)))
								jdata(i)(j) = null
							else
								jdata(i)(j) = data(i).asInstanceOf[Object]
							i += 1
						}
					}
					case v: REXPInteger => {
						val data = rcolvec.asIntegers()  // no allocation
						var i = 0  // row idx
						while (i < partitionSize) {
							if (REXPInteger.isNA(data(i)))
								jdata(i)(j) = null
							else
								jdata(i)(j) = data(i).asInstanceOf[Object]
							i += 1
						}
					}
					case v: REXPString => {
						val data = rcolvec.asStrings()  // no allocation
						var i = 0  // row idx
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

		(rddarrobj, meta)
	}
}
