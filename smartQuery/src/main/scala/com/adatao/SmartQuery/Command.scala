package com.adatao.SmartQuery

import com.adatao.pa.spark.DDF.content.Schema
import io.ddf.ml.IModel
import com.adatao.pa.ddf.spark.DDFManager
import com.adatao.pa.ddf.spark.DDF
import scala.collection.JavaConversions._
import io.ddf.types.AggregateTypes.AggregationResult

/**
 * author: daoduchuan
 */
sealed abstract class Task[T] {

  def execute(): T
}

/**
 * Query: use airline
 * @param ddfName
 */
case class UseDataset(ddfName: String) extends Task[String] {

  def execute(): String = {
    val ddf = Command.manager.getDDF(ddfName)
    Environment.currentDDF= ddf
    "Successful load ddf: " + ddfName + "\n" +
    "Columns: " + ddf.getColumnNames().mkString(", ") + "\n" +
    "Number of rows = " + ddf.nrow()
  }
}

/**
 * Query:
 * "List/show dataset in {directory} ddf://adatao"
 * "List/show dataset"
 * ""
 * @param directory
 */
case class ListDataset(directory: Option[String]) extends Task[String] {

  def execute(): String = {
    Command.manager.listDDFs()
  }
}

/**
 * Query: Show table in Hive/Parquet/Hbase/Cassandra
 */
case class ShowTables(database: String) extends Task[String] {

  def execute(): String = {
    if(database.equalsIgnoreCase("hive")) {
      Command.manager.sql("show tables").filter(table => !table.contains("sparkddf_spark_")).mkString("\n")
    } else {
      ""
    }
  }
}

/**
 * Query: Load month, day, year from table into/to myddf
 * Query: Load * from table into/to ddf
 * @param columns
 * @param tableName
 * @param datasetName
 */
case class LoadDataFromTable(columns: List[String], tableName: String, datasetName: String, filter: Option[Filtering] = None) extends Task[Unit] {

  def execute(): Unit = {
    val cmd = filter match {
      case Some(filtering) => s"select ${columns.mkString(", ")} from ${tableName} where" +
        s" ${filtering.left} ${filtering.comparison} ${filtering.right}"
      case None   => s"select ${columns.mkString(", ")} from ${tableName}"
    }
    val ddf = Command.manager.sql2ddf(cmd)
    ddf.setName(datasetName)
    println("Successful load dataset: " + datasetName)
  }
}


/**
 * query: Show relationship between arrdelay and depdelay
 * or "Show relationship between arrdelay and depdelay with year = 2007"
 * @param columns
 * @param filter
 */
case class RelationShipTask(columns: (String, String), filter: Option[Filtering]) extends Task[Unit] {

  def execute(): Unit = {
    val isValid = Environment.checkValidColumns(List(columns._1, columns._2))
    if(!isValid) {
      throw new Exception(s"Column ${columns._1} and ${columns._2} don't exist in current DDF, use command 'use DDF' to switch")
    }
    val ddf = filter match {
      case Some(filter) => {
        val expr = s"${filter.left} ${filter.comparison} ${filter.right}"
        Environment.currentDDF.filter(expr)
      }
      case None => Environment.currentDDF
    }
    val result: AggregationResult = ddf.aggregate(Array(columns._1), Array(columns._2), "mean")
    println(s"${columns._2}   ${columns._1}")
    result.foreach {
      case (str, Array(d))  => println(s"${str}      ${d}")
    }
  }
}

/**
 * Query:
 * "compare arrdelay and depdelay"
 * "compare arrdelay and depdelay with destination = LA"
 * @param col1
 * @param col2
 * @param schema
 */
case class ComparisonTask(col1: String, col2: String, schema: Schema) extends Task[Unit] {

  def execute(): Unit = {
    println("Running comparison task")
  }
}

case class GetVariable(variableName: String) extends Task[AnyRef] {

  def execute: AnyRef = {
    try {
      Environment.environment(variableName)
    } catch {
      case e: Exception => throw new Exception(s"Variable name ${variableName} does not exist")
    }
  }
}

/**
 * Query:
 * Use model to predict on ddf
 * @param getVariable
 * @param predict
 */
case class UsePredict(getVariable: GetVariable, predict: Predict) extends Task[Unit] {
  def execute(): Unit = {
    predict.model = getVariable.execute.asInstanceOf[IModel]
    val ddf = predict.execute()
    val data = ddf.sample(10, false, 17)
    data.foreach{
      row => println(row.mkString(", "))
    }
  }
}

/**
 * Query:
 * "``train to predict arrdelay from/with myddf"
 * "train how to predict arrdelay from/with ddf"
 * "learn to predict arrdelay from/with ddf"
 */
case class Train(trainColumn: String, dataset: String) extends Task[IModel] {

  def execute(): IModel = {
    val ddf = Command.manager.getDDF(dataset)
    val xCols = ddf.getColumnNames().filter(colName => colName != trainColumn)
    ddf.ML.LinearRegressionNormalEquation(xCols, trainColumn)
  }
}

/**
 * Query:
 * let model = train to predict arrdelay from/with ddf
 * @param assignment
 * @param train
 */
case class LetTrain(assignment: Assignment, train: Train) extends Task[Unit] {

  def execute(): Unit = {
    val model = train.execute()
    assignment.assign(model)
  }
}

/**
 * Query:
 * "use arrdelayModel to predict on airline"
 * do prediction then plot the result
 */

case class Predict(var model: IModel, dataset: String) extends Task[DDF] {

  def execute(): DDF = {
    val ddf = Command.manager.getDDF(dataset)
    ddf.applyModel(model)
  }
}

/**
 * Query:
 * "connect to localhost"
 * "connect to 192.186.1.1"
 * "connect to pa3.adatao.com"
 * @param serverHost
 */
case class Connect(serverHost: String) extends Task[Unit] {

  def execute(): Unit = {
    Command.manager = new DDFManager
    Command.manager.connect(serverHost)
    println("Successully connect to " + serverHost)
  }
}

object Command {
  private var _manager: DDFManager = null

  def manager = {
    if(_manager == null) {
      throw new Exception("No live session, issue connect command first")
    } else {
      _manager
    }
  }

  def manager_= (manager: DDFManager): Unit = _manager = manager
}
