package com.adatao.spark.ddf.content

import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.Schema.{ColumnType, Column}
import shark.memstore2.TablePartition
import java.util.{List => JList}
import shark.api.RDDTableFunctions
import scala.collection.JavaConversions._
import com.adatao.ddf.content.Schema
import scala.reflect.ClassTag
import com.adatao.ddf.exception.DDFException
import org.apache.hadoop.hive.ql.metadata.Hive
import shark.{SharkEnv, SharkContext}

/**
 * author: daoduchuan
 */

trait CanConvertToTablePartition[T] {
  def toTablePartition(rdd: RDD[T], columns: JList[Column], tableName: String): RDD[TablePartition]
}

object CanConvertToTablePartition {
  implicit object SeqCanConvertToTablePartition extends CanConvertToTablePartition[Seq[_]] {
    def toTablePartition(rdd: RDD[Seq[_]], columns: JList[Column], tableName: String): RDD[TablePartition]  = {

      val classTags = columns.map{col =>  getClassTagFromColumnType(col.getType)}
      val fields = columns.map{col => col.getName}
      val rddTableFunction = new RDDTableFunctions(rdd, classTags)

      try {
        rddTableFunction.saveAsTable(tableName, fields)
      } catch{
        case e: Throwable => throw new DDFException("Error getting RDD[TablePartition]", e)
      }

      val databaseName = Hive.get(SharkContext.hiveconf).getCurrentDatabase
      val memTableOrNull = SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName)

      if(memTableOrNull.isDefined) {
        try{
          memTableOrNull.get.getRDD.get
        } catch {
          case e: Throwable => throw new DDFException("Error getting RDD[TablePartition]", e)
        }
      } else {
        throw new DDFException("Error getting RDD[TablePartition]")
      }
    }
  }

  def getClassTagFromColumnType(mType: Schema.ColumnType): ClassTag[_] = {
    mType match {
      case ColumnType.STRING => ClassTag(classOf[String])
      case ColumnType.INT    => ClassTag(classOf[Int])
      case ColumnType.DOUBLE => ClassTag(classOf[Double])
      case ColumnType.LONG   => ClassTag(classOf[Long])
      case x                 => throw new DDFException("Unknown type " + x.toString)
    }
  }
}