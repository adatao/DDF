/*
 * Copyright (C) 2013 Adatao Inc.
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adatao.pa.spark

import java.util.{ArrayList => JavaArrayList, HashSet => JavaHashSet}
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, DDLWork, DropTableDesc}
import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.hadoop.hive.ql.exec.DDLTask
import org.apache.hadoop.hive.conf.HiveConf
import com.adatao.pa.spark.DataManager.{SharkDataFrame, DataFrame, MetaInfo}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import shark.memstore2.{TablePartitionBuilder, TablePartitionStats, TablePartition}
import org.apache.spark.Accumulable
import org.apache.spark.rdd.RDD
import shark.api.{Row, JavaTableRDD, JavaSharkContext}
import shark.{ConfVar, SharkConfVars, SharkEnv}
import scala.collection.mutable.ArrayBuffer
import shark.memstore2.column._
import org.apache.hadoop.hive.conf.HiveConf

// code is adapted from https://github.com/amplab/shark/pull/136

private[adatao] object SharkUtils {

//	/**
//	 * Create a in-memory Shark table,
//	 * it is the caller responsibility to add the result to DataManager
//	 * and discard this Object[] DataFrame (to release memory).
//	 *
//	 * Preferably, we won't even have DataFrame anymore if everything in DataManager is a SharkDataFrame.
//	 *
//	 * @return SharkDataFrame
//	 */
//	def createSharkDataFrame(dtf: DataFrame, sparkContext: JavaSharkContext): SharkDataFrame = {
//		val types = dtf.getMetaInfo.map(x => toHiveType(x.getType))
//
//		val sharkMetaInfos= dtf.getMetaInfo.map{
//			x => {
//				val typ= toHiveType(x.getType)
//				val metainfo= new MetaInfo(x.getHeader, typ, x.getColumnNo)
//				metainfo.setFactor(x.getFactor)
//			}
//		}
//		val statsAcc= SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
//
//		val rdd= dtf.table.rdd.mapPartitionsWithIndex{
//			case(partitionIndex, iter) => tablePartitionMapper(statsAcc, types, partitionIndex, iter)
//		}.persist()
//		val tableName= "bigrdf" + dtf.getUid.replace('-', '_')
//		val ok = createTableInHive(tableName, dtf.getMetaInfo)
//		if(!ok){
//			throw new RuntimeException("fail to create temporary table in Hive metastore")
//		}
//
//		SharkEnv.memoryMetadataManager.put(tableName, rdd)
//		try{
//			forceEval(rdd)
//		} catch{
//			case e => dropTableInHive(tableName)
//		}
//
//		SharkEnv.memoryMetadataManager.putStats(tableName, statsAcc.value.toMap)
//		val sharkDTF= new SharkDataFrame(rdd, sharkMetaInfos)
//		val table= sparkContext.sql2rdd(String.format("SELECT * from %s", tableName))
//		sharkDTF.setRDD(table)
//		sharkDTF.setTableName(tableName)
//		sharkDTF
//	}
//
//	def tablePartitionMapper(statsAcc:Accumulable[ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)],
//				types: Array[String], partitionIndex: Int, iter: Iterator[Array[AnyRef]]): Iterator[TablePartition]= {
//
//		val columnBuiders: Array[ColumnBuilder[_]]= types.map{typ =>
//			val builder= typeToColumnBuilder(typ)
//			builder.initialize(1000000)
//			builder
//		}
//
//		val objectInspector = getJavaPrimitiveObjectInspector(types)
//		var numRows: Long = 0
//		while(iter.hasNext){
//			val row = iter.next
//			var i = 0
//			while(i < types.length){
//				if(row(i) == null)
//					columnBuiders(i).append(row(i), objectInspector(i))
//				else
//					columnBuiders(i).append(row(i), objectInspector(i))
//				i+=1
//			}
//			numRows += 1
//		}
//
//		val buffer = columnBuiders.map{bld => bld.build}
//		statsAcc += Tuple2(partitionIndex, new TablePartitionStats(columnBuiders.map(_.stats), numRows))
//		Iterator(new TablePartition(numRows, buffer))
//	}
//
//	def forceEval(table: RDD[TablePartition]) = {
//		table.context.runJob(table, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
//	}
//
//	def getJavaPrimitiveObjectInspector(typ: Array[String]):  Array[PrimitiveObjectInspector] = {
//		typ.map(x => getJavaPrimitiveObjectInspector(x))
//	}
//
//	def getJavaPrimitiveObjectInspector(typ: String): PrimitiveObjectInspector = typ match {
//
//		case "java.lang.Integer" 		| "int" => PrimitiveObjectInspectorFactory.javaIntObjectInspector
//		case "java.lang.Double" | "double" => PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
//		case "java.lang.String" | "string" => PrimitiveObjectInspectorFactory.javaStringObjectInspector
//		case "java.lang.Float"   | "float"  => PrimitiveObjectInspectorFactory.javaFloatObjectInspector
//		case "java.lang.Boolean" | "boolean" => PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
//		case "java.lang.Byte"    | "byte"    => PrimitiveObjectInspectorFactory.javaByteObjectInspector
//		case _				=> throw new IllegalArgumentException("Unknown type: " + typ)
//	}
//
//	def typeToColumnBuilder(typ: String): ColumnBuilder[_] = typ match{
//
//		case "int"		=> new IntColumnBuilder
//		case "double" => new DoubleColumnBuilder
//		case "string" => new StringColumnBuilder
//		case "float" => new FloatColumnBuilder
//		case "boolean" => new BooleanColumnBuilder
//		case "byte"		=> new ByteColumnBuilder
//		case _				=> throw new IllegalArgumentException("Unknown type: " + typ)
//	}
//	def toHiveType(typ: String): String = typ match{
//
//		case "java.lang.Integer" | "int"   => "int"
//		case "java.lang.Double" | "double" => "double"
//		case "java.lang.String" | "string" => "string"
//		case "java.lang.Float" | "float" => "float"
//		case "java.lang.Boolean" | "boolean" => "boolean"
//		case "java.lang.Byte" | "byte" => "byte"
//		case _				=> throw new IllegalArgumentException("Unknown type: " + typ)
//	}
//	/**
//	 * Execute the create table DDL operation against Hive's metastore.
//	 */
//	def createTableInHive(tableName: String, dfMeta: Array[MetaInfo]): Boolean = {
//		val schema = dfMeta.map { colMeta =>
//			new FieldSchema(colMeta.getHeader, toHiveType(colMeta.getType), "")
//		}
//
//		// Setup the create table descriptor with necessary information.
//		val createTbleDesc = new CreateTableDesc()
//		createTbleDesc.setTableName(tableName)
//		createTbleDesc.setCols(new JavaArrayList[FieldSchema](schema.toList))
//		createTbleDesc.setTblProps(Map("shark.cache" -> "heap"))
//		createTbleDesc.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
//		createTbleDesc.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
//		createTbleDesc.setSerName(classOf[shark.memstore2.ColumnarSerDe].getName)
//		createTbleDesc.setNumBuckets(-1)
//		val hiveConf= new HiveConf()
//		// Execute the create table against the metastore.
//		val ddlWork = new DDLWork(new JavaHashSet[ReadEntity],
//			new JavaHashSet[WriteEntity],
//			createTbleDesc)
//		val taskExecutionStatus = executeDDLTaskDirectly(ddlWork, hiveConf)
//		return (taskExecutionStatus == 0)
//	}
//
//	def dropTableInHive(tableName: String): Boolean = {
//		// Setup the drop table descriptor with necessary information.
//		val dropTblDesc = new DropTableDesc(
//			tableName,
//			false /* expectView */,
//			false /* ifExists */,
//			false /* stringPartitionColumns */)
//
//		// Execute the drop table against the metastore.
//		val ddlWork = new DDLWork(new JavaHashSet[ReadEntity],
//			new JavaHashSet[WriteEntity],
//			dropTblDesc)
//		val taskExecutionStatus = executeDDLTaskDirectly(ddlWork)
//		return (taskExecutionStatus == 0)
//	}
//
//	def executeDDLTaskDirectly(ddlWork: DDLWork): Int = {
//		val task = new DDLTask()
//		task.initialize(new HiveConf, null, null)
//		task.setWork(ddlWork)
//
//		// Hive returns 0 if the create table command is executed successfully.
//		return task.execute(null)
//	}
//
//	def executeDDLTaskDirectly(ddlWork: DDLWork, hiveConf: HiveConf) = {
//		val task = new DDLTask
//		task.initialize(hiveConf, null, null)
//		task.setWork(ddlWork)
//		task.execute(null)
//	}
//	
//	class SharkParsePoint(xCols: Array[Int], metaInfo: Array[DataManager.MetaInfo]) extends Function1[Row, Array[Double]]
//  with Serializable
//  {
//
//    val base0_XCols: ArrayBuffer[Int] = new ArrayBuffer[Int]
//    xCols.foreach(x => base0_XCols += x)
//
//    override def apply(row: Row): Array[Double] = row match {
//      case null =>  null
//      case x => {
//        val dim = base0_XCols.length
//        val result= new ArrayBuffer[Double]
//        var i = 0
//        while(i < dim){
//          val idx = base0_XCols(i)
//          metaInfo(idx).getType() match{
//            case "int" => row.getInt(idx) match{
//              case null => return null
//              case x =>    result += x.toDouble
//            }
//            case "double" => row.getDouble(idx) match{
//              case null => return null
//              case x => result += x
//            }
//            case s => throw new Exception("not supporting type" + s)
//          }
//          i+=1
//        }
//        result.toArray
//      }
//    }
//  }
}
