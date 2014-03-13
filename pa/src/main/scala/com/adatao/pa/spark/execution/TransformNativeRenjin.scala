package adatao.bigr.spark.execution

import org.renjin.script.RenjinScriptEngineFactory

import org.apache.spark.rdd.RDD
import adatao.bigr.spark.DataManager.{DataFrame, SharkDataFrame, MetaInfo}
import org.renjin.sexp._
import scala.Some
import scala.collection.mutable
import shark.memstore2.TablePartition
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.renjin.primitives.vector.RowNamesVector

/**
 * This executor performs transformation of BigDataFrame using a native R function:
 * 	 - add column using a function(row) {...}
 * 	 - transform existing column
 * 	 - user's expression can access underlying per-partition data.frame named 'df.partition'
 *
 * Author: aht
 */
class TransformNativeRenjin(dataContainerID: String, val transformExpr: String) extends AExecutor[DataFrameResult] {
	override def runImpl(context: ExecutionContext): DataFrameResult = {
		val dm = context.sparkThread.getDataManager
		Option(dm.get(dataContainerID)) match {
			case Some(dataContainer) ⇒ dataContainer match {
				case sdf: SharkDataFrame => {
					val rddpartdf = TransformNativeRenjin.asRenjin(sdf.getTablePartitionRDD, sdf.getMetaInfo).map { partdf =>
						// now we've got a Renjin df of the partition data in Java
						// we need to prepare the Renjin environment then invoke R code on it
						val engine = new RenjinScriptEngineFactory().getScriptEngine

						val dfvarname = "df.partition"
						engine.put(dfvarname, partdf)

						val expr = String.format("%s <- transform(%s, %s)", dfvarname, dfvarname, transformExpr)
						LOG.info("eval expr = {}", expr)
						try {
							val partdfres = engine.eval(expr)

							// this only print a shortened form
							// println("AHT !!! resulting df.partition = " + partdfres)

							// uncomment this to print whole content of the df.partition for debug
							// engine.eval(String.format("print(%s)", dfvarname))

							partdfres.asInstanceOf[ListVector]
						} catch {
							case e: Exception => throw new RuntimeException("failed to evaluate " + expr + ", error: " + e.toString)
						}
					}

					val (rdd, meta) = TransformNativeRenjin.asJava(rddpartdf)

					// add the rdd to data manager
					val bigdf = new DataFrame(meta, rdd)
					val uid = dm.add(bigdf)

					new DataFrameResult(uid, meta)
				}
				case _ => throw new IllegalArgumentException("can only add data to a Hive-backed BigDataFrame")
			}
			case _ => throw new IllegalArgumentException("dataContainerID not found")
		}
	}
}

object TransformNativeRenjin {
	// transform a bigr data frame into a rdd of native renjin dataframes per partition (as Java objects)
	// note that a R data frame is just a list of column vectors with additional attributes
	def asRenjin(rdd: RDD[TablePartition], metaInfo: Array[MetaInfo]): RDD[ListVector] = {
		rdd.filter { tp => tp.iterator.columnIterators.length > 0 }.map { tp =>
			println("AHT !!! tp.numRows = "+tp.numRows)

			// Assemble the Renjin data frame
			// rdata is a ListVector which contains SEXP which are columnar AtomicVector of int|double|string
			val rdata = new mutable.ArrayBuffer[SEXP]

			metaInfo.map { colMeta =>
				val iter = tp.iterator.columnIterators(colMeta.getColumnNo)
				val rvec = colMeta.getType match {
					case "int" => {
						val builder = new IntArrayVector.Builder(tp.numRows.asInstanceOf[Int])
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								builder.set(i, iter.current.asInstanceOf[IntWritable].get)
							else
								builder.setNA(i)
							i += 1
						}
						builder.build()
					}
					case "double" => {
						val builder = new DoubleArrayVector.Builder(tp.numRows.asInstanceOf[Int])
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								builder.set(i, iter.current.asInstanceOf[DoubleWritable].get)
							else
								builder.setNA(i)
							i += 1
						}
						builder.build()
					}
					case "string" => {
						val builder = new StringVector.Builder(tp.numRows.asInstanceOf[Int])
						var i = 0
						while (i < tp.numRows) {
							iter.next()
							if (iter.current != null)
								builder.set(i, iter.current.asInstanceOf[Text].toString)
							else
								builder.setNA(i)
							i += 1
						}
						builder.build()
					}
				}
				rdata += rvec.asInstanceOf[SEXP]
			}

			val rattributes = AttributeMap.builder()
			rattributes.setNames(new StringArrayVector(metaInfo.map(mi => mi.getHeader), AttributeMap.EMPTY))

			// make it a data.frame by setting the R class and row.names
			rattributes.setClass("data.frame")
			rattributes.set(Symbols.ROW_NAMES, new RowNamesVector(tp.numRows.asInstanceOf[Int], AttributeMap.EMPTY))

			// this is the per-partition Renjin data.frame
			new ListVector(rdata.toArray, rattributes.build())
		}
	}

	def asJava(rdd: RDD[ListVector]): (RDD[Array[Object]], Array[MetaInfo]) = {

		// convert R types to Java
		val first = rdd.first()
		val names = first.getNames()
		// detect the output type
		val meta = new Array[MetaInfo](first.length)
		for (j <- 0 until first.length) {
			val bigrType = first.get(j) match {
				case v: DoubleVector => "double"
				case v: IntVector => "int"
				case v: StringVector => "string"
				case _ => throw new IllegalArgumentException("BigR only support atomic vectors of type int|double|string")
			}
			meta(j) = new MetaInfo(names.getElementAsString(j), bigrType, j)
		}

		val rddarrobj = rdd.flatMap { partdf =>
			val partitionSize = partdf.maxElementLength

			println("AHT !!! partdf.len = " + partdf.length())
			println("AHT !!! partitionSize = " + partitionSize)

			// big allocation!
			val jdata = Array.ofDim[Object](partitionSize, partdf.length())

			// convert R column-oriented AtomicVector to row-oriented Object[]
			// TODO: would be nice to be able to create BigR DataFrame from columnar vectors
			var i = 0  // row idx
			while (i < partitionSize) {
				var j = 0  // column idx
				while (j < partdf.length()) {
					val colvec = partdf.getElementAsSEXP(j).asInstanceOf[AtomicVector]
					// Renjin has custom representation of NA for Int and Double types
					if (colvec.isElementNA(i))
						jdata(i)(j) = null
					else
						jdata(i)(j) = colvec.getElementAsObject(i)
					j += 1
				}
				// println("data(i) = " + util.Arrays.toString(jdata(i)))
				i += 1
			}
			jdata
		}

		(rddarrobj, meta)
	}
}
