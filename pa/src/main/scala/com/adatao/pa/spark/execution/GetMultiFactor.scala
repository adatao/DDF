package com.adatao.pa.spark.execution


import scala.collection.JavaConversions._
import java.util.{HashMap => JMap, Map}
import java.util
import java.lang.{Integer => JInt}
import com.adatao.pa.spark.execution.GetMultiFactor.{MultiFactorMapper, SharkMultiFactorMapper, MultiFactorReducer}
import com.adatao.pa.spark.DataManager.{MetaInfo, DataFrame, SharkDataFrame}
import shark.memstore2.TablePartition
import org.apache.hadoop.io.{LongWritable, FloatWritable, IntWritable, Text}
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import com.adatao.pa.spark.Utils._
import scala.Some
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 *
 */
class GetMultiFactor(dataContainerID: String,
										 var columnIndexs: Array[Int]= null)
		extends AExecutor[Array[(JInt, java.util.Map[String, JInt])]]{

	protected override def runImpl(context: ExecutionContext): Array[(JInt, java.util.Map[String, JInt])] = {

    val ddf = context.sparkThread.getDDFManager.getDDF(getDDFNameFromDataContainerID(dataContainerID))

    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF " + dataContainerID, null)
    }
    val schemaHandler = ddf.getSchemaHandler
    for (columnIndex <- columnIndexs) {
      schemaHandler.setAsFactor(columnIndex)
    }
    schemaHandler.getFactorLevelsAndLevelCounts()

    val result: Array[(JInt, java.util.Map[String, JInt])] = for {
      columnIndex <- columnIndexs
      colName = schemaHandler.getColumnName(columnIndex)
    } yield ((JInt.valueOf(columnIndex), schemaHandler.getColumn(colName).getOptionalFactor.getLevelCounts))

    result
	}
}

object GetMultiFactor {
	def stringGetter(typ: String): (Option[Object]) => Option[String] = typ match {
		case "int" | "java.lang.Integer"		=> {
			case Some(ob) => Option(ob.asInstanceOf[IntWritable].get.toString)
			case None 	  => None
		}
		case "double" | "java.lang.Double" => {
			case Some(ob) => Option(ob.asInstanceOf[DoubleWritable].get.toString)
			case None 		=> None
		}
		case "float" | "java.lang.Float" =>  {
			case Some(ob)	=> Option(ob.asInstanceOf[FloatWritable].get.toString)
			case None 	=> None
		}
		case "bigint" => {
			case Some(ob)	=> Option(ob.asInstanceOf[LongWritable].get.toString)
			case None 		=> None
		}
		case "string" | "java.lang.String" => {
			case Some(ob)	=> Option(ob.asInstanceOf[Text].toString)
			case None 	=> None
		}
	}

	class SharkMultiFactorMapper(indexsWithTypes: Array[(Int, String)])
			extends Function1[TablePartition, JMap[Int, JMap[String, JInt]]] with Serializable {
		@Override
		def apply(table: TablePartition): JMap[Int, JMap[String, JInt]] = {
			val aMap: JMap[Int, JMap[String, JInt]] = new java.util.HashMap[Int, JMap[String, JInt]]()
			val columnIterators= table.iterator.columnIterators
			val numRows= table.numRows

			//Iterating column base, faster with TablePartition
			for((idx, typ) <- indexsWithTypes){
				val newMap = new JMap[String, JInt]()
				val columnIter= columnIterators(idx)
				var i = 0
				val getter= stringGetter(typ)
				while(i < numRows){
					columnIter.next()
					val value = getter(Option(columnIter.current))
					value match {
						case Some(string) => {
							val num = newMap.get(string)
							newMap.put(string, if(num == null) 1 else (num + 1))
						}
						case None =>
					}
					i += 1
				}
				aMap.put(idx, newMap)
			}
			aMap
		}
	}

	class MultiFactorMapper(indexsWithTypes: Array[(Int, String)])
			extends Function1[Iterator[Array[Object]], Iterator[JMap[Int, JMap[String, JInt]]]] with Serializable {
		@Override
		def apply(iter: Iterator[Array[Object]]): Iterator[JMap[Int, JMap[String, JInt]]] = {
			val aMap: JMap[Int, JMap[String, JInt]] = new java.util.HashMap[Int, JMap[String, JInt]]()
			while(iter.hasNext){
				val row = iter.next()
				val typeIter= indexsWithTypes.iterator
				while(typeIter.hasNext){
					val(idx, typ) = typeIter.next()
					val value: Option[String] = Option(row(idx)) match{
				 		case Some(x)	=> typ match {
							case "java.lang.Integer"| "int" 	 => Option(x.asInstanceOf[Int].toString)
							case "java.lang.Double" | "double" => Option(x.asInstanceOf[Double].toString)
							case "java.lang.String" | "string" => Option(x.asInstanceOf[String])
							case "java.lang.Float"  | "float"  => Option(x.asInstanceOf[Float].toString)
							case "Unknown" 					=> x match{
								case y: java.lang.Integer		 => Option(y.toString)
								case y: java.lang.Double 		 => Option(y.toString)
								case y: java.lang.String 		 => Option(y)
							}
						}
						case None 		=> None
					}
					value match {
						case Some(string) => {
							Option(aMap.get(idx)) match {
								case Some(map) => {
									val num = map.get(string)
									map.put(string, if(num == null) 1 else num +1)
								}
								case None 		=> {
									val newMap = new util.HashMap[String, JInt]()
									newMap.put(string, 1)
									aMap.put(idx, newMap)
								}
							}
						}
						case None =>
					}
				}
			}
			Iterator(aMap)
		}
	}

	class MultiFactorReducer
		extends Function2[JMap[Int, JMap[String, JInt]], JMap[Int, JMap[String, JInt]],
				JMap[Int, JMap[String, JInt]]]
			with Serializable {
		@Override
		def apply(map1:  JMap[Int, JMap[String, JInt]], map2:  JMap[Int, JMap[String, JInt]]):
					JMap[Int, JMap[String, JInt]] = {
				for((idx, smap1) <- map1) {

					Option(map2.get(idx)) match {
							case Some(smap2) =>
								for((string, num) <- smap1){
									val aNum = smap2.get(string)
									smap2.put(string, if(aNum == null) num else (aNum + num))
							}
						case None 			=>  map2.put(idx, smap1)
					}
				}
			map2
		}
	}
}
