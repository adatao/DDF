package com.adatao.SmartQuery

import scala.util.parsing.combinator.{PackratParsers, JavaTokenParsers}
import io.ddf.ml.IModel

/**
 */

class Parser extends JavaTokenParsers {

  // match a single word
  def aSingleWord: Parser[String] = """(\w+)""".r

  def integer: Parser[String] = """^\d+$""".r
  def hostName_delimiter: Parser[String] = "-" | "."

  def host: Parser[String] = {
    (rep(aSingleWord ~ hostName_delimiter).? ~ aSingleWord) ^^ {
      case list ~ aWord =>  {
        list match {
          case Some(ls) =>
            var lsString = ""
            ls.foreach{
              case a ~ b =>  lsString += a + b
            }
            lsString + aWord
          case None => aWord
        }
      }
    }
  }

  def column: Parser[String] = aSingleWord

  def directory: Parser[String] = "ddf://" ~ repsep(aSingleWord, "/") ~ "/".? ^^ {
    case parent ~ ls ~ opt => parent + ls.mkString("/") + opt.getOrElse("")
  }

  def comparison: Parser[String] = "=" | ">" | "<" | "<=" | ">="

  //match a filter expression
  def filter: Parser[Filtering] =
    (column ~ comparison ~ column) ^^ {
    case left ~ compare ~ right => Filtering(left, right, compare)
  }

  def useDataset: Parser[Task[String]] = "use" ~> aSingleWord ^^ {
    case dataset => UseDataset(dataset)
  }

  def listDataset: Parser[Task[String]] = {
    "list" ~> ((("dataset" | "data") ~ "in" ~ "directory".?) ~> directory.?) ^^ {
      case someDirectory => ListDataset(someDirectory)
    }
  }

  def showTables: Parser[Task[String]] = {
    ("show" ~ ("tables" | "table") ~ "in") ~> ("hive" | "parquet" | "hbase" | "cassandra") ^^ {
      case database => ShowTables(database)
    }
  }

  def loadTable: Parser[Task[_]] = {
    ("load" ~> repsep(column, ",")) ~
    ("from" ~> aSingleWord) ~
    (("into" | "to") ~> aSingleWord) ~ ("with" | "where").? ~ filter.? ^^ {
      case columns ~ table ~ dataset ~ w ~ filtering =>  LoadDataFromTable(columns, table, dataset, filtering)
    }
  }

  def relationship: Parser[Task[Unit]] = {
    "show relationship" ~>
    ("between".? ~> (column ~ ("and" | "&") ~ column)) ~ ("with" | "where" ).? ~
    filter.? ^^ {
      case a ~ and ~ b ~ w ~ filter => {
        RelationShipTask((a, b), filter)
      }
    }
  }

  def train: Parser[Train] = {
    (("train"| "learn") ~ "to predict") ~> (aSingleWord ~ ("from" | "with" | "on") ~ aSingleWord) ^^ {
      case trainColumn ~ from ~ dataset => Train(trainColumn, dataset)
    }
  }

  def letStatement: Parser[Assignment] = "let" ~> (aSingleWord <~ "=".?)  ^^ {
    case variable => Assignment(variable)
  }

  def letTrain: Parser[Task[Unit]] = {
    letStatement ~ train ^^ {
      case let ~ trainStatement => LetTrain(let, trainStatement)
    }
  }

  def useVariable: Parser[GetVariable] = "use" ~> aSingleWord ^^ {
    case variable => GetVariable(variable)
  }

  def predict: Parser[Predict] = {
    ("predict on" | "predict") ~> aSingleWord ^^ {
      case dataset => new Predict(null, dataset)
    }
  }

  def usePredict: Parser[Task[Unit]]  = {
    useVariable ~ "to".? ~ predict ^^ {
      case use ~ to ~ prediction => {
        UsePredict(use, prediction)
      }
    }
  }

  def connect(): Parser[Connect] = {
    ("connect" ~ "to".?) ~> host ^^ {
      case hostName => new Connect(hostName)
    }
  }

  def operations: Parser[Task[_]] =
    connect |
    usePredict |
    useDataset |
    listDataset |
    showTables |
    loadTable  |
    relationship |
    train |
    letTrain

  def parse(input: String): Task[_] = parseAll(operations, input.toLowerCase) match {
    case Success(result, _) => result
    case Failure(msg, _) => throw new Exception("Failed to parse " + input + ", messsage = " + msg)
  }

  def driverLoop(): Unit = {
    while(true) {
      val input = scala.Console.readLine()
      println(input)
      val output = this.parse(input)
    }
  }
}

object SQParser extends Parser {

  def eval(input: String): Any = {
    this.parse(input).execute()
  }
}
