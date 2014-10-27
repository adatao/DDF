package com.adatao.SmartQuery

import scala.util.parsing.combinator.JavaTokenParsers
import io.ddf.ml.IModel

/**
 */

class SmartQueryParser extends JavaTokenParsers {

  // match a single word
  def aSingleWord: Parser[String] = """(\w+)""".r

  def column: Parser[String] = aSingleWord

  def directory: Parser[String] = "ddf://" ~ repsep(aSingleWord, "/") ~ "/".? ^^ {
    case parent ~ ls ~ opt => parent + ls.mkString("/") + opt.getOrElse("")
  }

  def comparision: Parser[String] = "=" | ">" | "<" | "<=" | ">="

  //match a filter expression
  def filter: Parser[Filtering] = "with" ~>
    (column ~ comparision ~ column) ^^ {
    case left ~ compare ~ right => Filtering(left, right, compare)
  }

  def letStatement: Parser[Assignment] = "let" ~> (aSingleWord <~ "=".?)  ^^ {
    case variable => Assignment(variable)
  }

  def useDataset: Parser[Task[Unit]] = "use" ~> aSingleWord ^^ {
    case dataset => UseDataset(dataset)
  }

  def listDataset: Parser[Task[String]] = {
    "list" ~> ((("dataset" | "data") ~ "in" ~ "directory".?) ~> directory.?) ^^ {
      case someDirectory => ListDataset(someDirectory)
    }
  }

  def showTables: Parser[Task[String]] = {
    "show table in" ~> ("hive" | "parquet" | "hbase" | "cassandra") ^^ {
      case database => ShowTables(database)
    }
  }

  def loadTable: Parser[Task[_]] = {
    ("load" ~> repsep(column, ",")) ~
    ("from" ~> aSingleWord) ~
    (("into" | "to") ~> aSingleWord) ^^ {
      case columns ~ table ~ dataset => LoadDataFromTable(columns, table, dataset)
    }
  }

  def relationship: Parser[Task[Unit]] = {
    "show relationship" ~>
    ("between".? ~> (column ~ ("and" | "&") ~ column)) ~
    filter.? ^^ {
      case a ~ someWord ~ b ~ filter => {
        RelationShipTask((a, b), filter)
      }
    }
  }

  def train: Parser[Task[IModel]] = {
    "train to predict" ~> (aSingleWord ~ "from" ~ aSingleWord) ^^ {
      case trainColumn ~ "from" ~ dataset => Train(trainColumn, dataset)
    }
  }

  def letTrain: Parser[Unit] = {
    letStatement ~ train ^^ {
      case let ~ trainStatement => {
        val model = trainStatement.execute()
        let.assign(model)
      }
    }
  }

  def useVariable: Parser[AnyRef] = "use" ~> aSingleWord ^^ {
    case variable => GetVariable
  }

  def predict: Parser[Task[Unit]] = {
    "use" ~
  }

  def operations: Parser[Task[_]] =
    useDataset |
    listDataset |
    showTables |
    loadTable  |
    relationship

  def eval(input: String): Task[_] = parseAll(operations, input.toLowerCase) match {
    case Success(result, _) => result
    case Failure(msg, _) => throw new Exception("Failed to parse " + input)
  }

  def driverLoop(): Unit = {
    while(true) {
      val input = scala.Console.readLine()
      println(input)
      val output = this.eval(input)
    }
  }
}
