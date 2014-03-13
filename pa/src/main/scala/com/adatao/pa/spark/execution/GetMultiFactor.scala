package adatao.bigr.spark.execution


import scala.collection.JavaConversions._
import java.util.{HashMap => JMap}
import java.util
import java.lang.{Integer => JInt}
import adatao.bigr.spark.execution.GetMultiFactor.{MultiFactorMapper, SharkMultiFactorMapper, MultiFactorReducer}
import adatao.bigr.spark.DataManager.{MetaInfo, DataFrame, SharkDataFrame}
import shark.memstore2.TablePartition
import org.apache.hadoop.io.{LongWritable, FloatWritable, IntWritable, Text}
import org.apache.hadoop.hive.serde2.io.DoubleWritable

/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 *
 */
class GetMultiFactor(dataContainerID: String,
										 var columnIndexs: Array[Int]= null)
		extends AExecutor[Array[(Int, JMap[String, JInt])]]{

	protected override def runImpl(context: ExecutionContext): Array[(Int, JMap[String, JInt])] ={

		val df = context.sparkThread.getDataManager.get(dataContainerID)
		if(df == null) throw new IllegalArgumentException("dataContainerID doesn't exist in user session")
    val metaInfos= df.getMetaInfo

		require(columnIndexs != null, "Please supply columnIndexs or columnNames")

    val outOfBoundCols= columnIndexs.filter(idx => idx >= metaInfos.length)
		if(outOfBoundCols.size > 0)
      throw new IllegalArgumentException(String.format("Columns " + outOfBoundCols.mkString(", ")+ " is out of bound."))

		val indexsWithTypes= for{
			idx <- columnIndexs
		}yield(idx, metaInfos(idx).getType)
		LOG.info("indexwithType= " + indexsWithTypes.mkString(", "))

		val factor = df match {
			case sdf: SharkDataFrame =>  {
				val mapper= new SharkMultiFactorMapper(indexsWithTypes)
				sdf.getTablePartitionRDD.filter(table => table.numRows > 0).map(mapper).reduce(new MultiFactorReducer)
			}
			case xdf: DataFrame      =>  {
				val mapper = new MultiFactorMapper(indexsWithTypes)
				xdf.getRDD.rdd.mapPartitions(mapper).reduce(new MultiFactorReducer)
			}
		}
		//set factor for MetaInfo
		factor.foreach{
			case(idx,map) => {
				metaInfos(idx).setFactor(map)
			}
		}

		for{
			idx <- columnIndexs
		}yield((idx, factor(idx)))
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
