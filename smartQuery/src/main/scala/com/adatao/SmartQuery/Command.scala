package com.adatao.SmartQuery

import com.adatao.pa.spark.DDF.content.Schema
import io.ddf.ml.IModel

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
case class UseDataset(ddfName: String) extends Task[Unit] {

  def execute(): Unit = {
    println("Running use dataset")
  }
}

/**
 * Query: List dataset in directory /user/adatao
 * List dataset
 * @param directory
 */
case class ListDataset(directory: Option[String]) extends Task[String] {

  def execute(): String = {
    println("Running list dataset")
    "list dataset"
  }
}

/**
 * Query: Show table in Hive/Parquet/Hbase/Cassandra
 */
case class ShowTables(database: String) extends Task[String] {

  def execute(): String = {
    println("Running show tables")
    "show tables"
  }
}

/**
 * Query: Load month, day, year from table into/to myddf
 * @param columns
 * @param tableName
 * @param datasetName
 */
case class LoadDataFromTable(columns: List[String], tableName: String, datasetName: String) extends Task[Unit] {

  def execute(): Unit = {

    println("Running load data from table")
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
    println("Running relationship task")
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

/**
 * Query:
 * train to predict arrdelay from myddf"
 */
case class Train(trainColumn: String, dataset: String) extends Task[IModel] {

  def execute(): IModel = {
    println("Running Train")
    null
  }
}

/**
 * Query:
 * "use arrdelayModel to predict on airline"
 * do prediction then plot the result
 */

case class Predict(model: IModel, dataset: String) extends Task[Unit] {

  def execute(): Unit = {

    println("Running predict")
  }
}
