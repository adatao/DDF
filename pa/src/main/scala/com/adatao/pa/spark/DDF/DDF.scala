package com.adatao.pa.spark.DDF

import com.adatao.pa.spark.DataManager.MetaInfo
import com.adatao.ddf.ml.{RocMetric, IModel}
import com.adatao.pa.spark.execution._
import com.adatao.pa.spark.Utils.DataFrameResult
import com.adatao.pa.spark.execution.QuickSummary.DataframeStatsResult
import com.adatao.pa.spark.execution.NRow.NRowResult
import com.adatao.pa.spark.DDF.DDFManager.client
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult
import com.adatao.pa.spark.DDF.content.Schema
import java.util.{List => JList}
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import com.adatao.pa.spark.DDF.analytics.MLFacade
import com.adatao.ddf.content.Schema.Column
import scala.collection.JavaConversions._
import com.adatao.pa.spark.DDF.content.SchemaHandler
import com.adatao.pa.spark.execution.SetDDFName.SetDDFNameResult
import java.util.ArrayList
import org.apache.commons.lang.StringUtils
import com.adatao.pa.spark.execution.Subset.SubsetResult
import com.adatao.pa.spark.execution.FiveNumSummary.ASummary

/**
 * author: daoduchuan
 */
class DDF(var name: String, var columns: Array[Column]) {

  private var isMutable: Boolean = false

  def this(name: String, metainfo: Array[MetaInfo]) = {
    this(name, metainfo.map(info => DDF.metaInfoToColumn(info)))
  }

  val ML: MLFacade = new MLFacade(this)

  private val Schema: Schema = new Schema(this, this.columns)

  private val schemaHandler: SchemaHandler = new SchemaHandler(this)

  def getColumnNames(): Array[String] = {
    Schema.getColumnNames()
  }

  def nrow(): Long = {
    Schema.getNumRows()
  }

  def ncol(): Int = {
    Schema.getNumColumns()
  }

  def getSchema() = {
    this.Schema
  }

  def getSchemaHandler() = {
    this.schemaHandler
  }

  def setName(ddfName: String): String = {
    val cmd = new SetDDFName(this.name, ddfName)
    this.name = ddfName
    client.execute[SetDDFNameResult](cmd).result.uri
  }

  def fetchRows(numRows: Int): String = {
    val cmd = new FetchRows
    cmd.setDataContainerID(this.name)
    cmd.setLimit(numRows)
    val result = client.execute[FetchRowsResult](cmd).result
    val ls = result.getData
    ls.mkString("\n")
  }
  
  def top(oColumns: List[String], numRows: Int =10, mode: String = "asc"): String = {
    val cmd = new TopN
    cmd.setDataContainerID(this.name)
    cmd.setLimit(numRows)
    cmd.setMode(mode)
    
    var orderColumns: String ="";
    orderColumns = oColumns(0);
    var i :Int = 1
    while( i< oColumns.size) {
      orderColumns += "," + oColumns(i)
      i += 1
    }
    
    cmd.setOrderedCols(orderColumns)
    
    
    val result = client.execute[FetchRowsResult](cmd).result
    val ls = result.getData
    ls.mkString("\n")
  }
  
  def summary(): DataframeStatsResult = {
    val cmd = new QuickSummary
    cmd.setDataContainerID(this.name)
    client.execute[DataframeStatsResult](cmd).result
  }

  def fivenum(): Unit = {
    val cmd = new FiveNumSummary(this.name)
    val result= client.execute[Array[ASummary]](cmd).result
    val indent = "\t"
    var str = s"column${indent + indent}min${indent}max${indent}first_quartile${indent}median${indent}third_quartile\n"
    val resultZipColumns = result zip this.getColumnNames()
    resultZipColumns.foreach {
      case (fivenum, col) => {
        val row = s"$col${indent}${fivenum.min}${indent}${fivenum.max}${indent}${fivenum.first_quartile}${indent + indent}${fivenum.median}" +
          s"${indent}${fivenum.third_quartile}\n"
        str = str ++ row
      }
    }
    print(str)
    result
  }

  def applyModel(model: IModel): DDF = {
    val cmd = new YtrueYpred(this.name, model.getName)
    val result = client.execute[YtrueYpredResult](cmd).result
    new DDF(result.dataContainerID, result.metaInfo)
  }

  def roc(alpha_length: Int): RocMetric = {
    val cmd = new ROC(this.name, alpha_length)
    client.execute[RocMetric](cmd).result
  }

  def setMutable(isMutable: Boolean): DDF = {
    this.isMutable = isMutable
    val cmd = new MutableDDF(this.name, isMutable)
    client.execute[Sql2DataFrameResult](cmd).result
    this
  }

  def transform(transformExp: String): DDF = {
    val cmd = new TransformNativeRserve(this.name, transformExp)
    val dataFrameResult = client.execute[DataFrameResult](cmd).result
    new DDF(dataFrameResult.dataContainerID, dataFrameResult.metaInfo)
  }

  def groupBy(groupedColumns: List[String], selectedFucntion: List[String]): DDF = {
    val cmd = new GroupBy(this.name, groupedColumns, selectedFucntion)
    val result = client.execute[com.adatao.pa.spark.Utils.DataFrameResult](cmd).result
    new DDF(result.getDataContainerID, result.getMetaInfo)
  }

  def binning(column: String, binningType: String, numBins: Int = 0, breaks: Array[Double] = null,
               includeLowest: Boolean = false, right: Boolean= true, decimalPlaces: Int = 2): DDF = {
    val cmd = new Binning(this.name, column, binningType, numBins, breaks, includeLowest, right, decimalPlaces)
    val result = client.execute[BinningResult](cmd).result
    new DDF(result.dataContainerID, result.metaInfo)
  }

  def dropNA(axis: String = "row", how: String = "any", threshold: Long = 0L, columns: JList[String]= null): DDF = {
    val cmd = new DropNA(axis, how, threshold, columns, this.name)
    val result = client.execute[DataFrameResult](cmd).result
    if(isMutable) {
      this.name = result.getDataContainerID
      this.columns = result.getMetaInfo.map(info => DDF.metaInfoToColumn(info))
      this
    } else {
      new DDF(result.dataContainerID, result.getMetaInfo)
    }
  }
  
  def project(projectColumns: Array[String]): DDF = {
    val dcID: String = this.name
    var i =0
    var xCols: Array[Int] = new Array[Int] (projectColumns.length)
    
    while(i < projectColumns.length) {
      var j:Int =0 
      while(j < this.getColumnNames.length) {
        if(this.getColumnNames.apply(j).equals(projectColumns(i)))
          xCols(i) = j
        j += 1
      }
      i += 1
    }
    
    val columnList = new ArrayList[String]
    for (xCol <- xCols) {
      columnList.add("{type: Column, index: " + xCol + "}")
    }
    val jsCreateVectors = String.format("{columns: [%s], dataContainerID: %s}", StringUtils.join(columnList, ", "), dcID);
    val result= client.execute[SubsetResult]("Subset", jsCreateVectors)
    
    
     new DDF(result.result.getDataContainerID, result.result.getMetaInfo) 
  }
}

object DDF {
  def metaInfoToColumn(metainfo: MetaInfo): Column = {
    val col = new Column(metainfo.getHeader, metainfo.getType)
    if(metainfo.hasFactor) {
      col.setAsFactor(null)
    }
    col
  }
}
