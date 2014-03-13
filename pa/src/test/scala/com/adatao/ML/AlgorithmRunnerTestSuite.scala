package com.adatao.ML

import scala.collection.JavaConversions._

import org.scalatest.FunSuite

import org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel

import shark.api.QueryExecutionException
//import shark.memstore2.{ CacheType, MemoryMetadataManager, PartitionedMemoryTable }
//import shark.tgf.{ RDDSchema, Schema }
//
//import shark.SharkRunner._

class AlgorithmRunnerTestSuite extends FunSuite {
/*  var sc: SharkContext = SharkRunner.init()
  var sharkMetastore: MemoryMetadataManager = SharkEnv.memoryMetadataManager
  object TestTGF1 {

    def apply(test: RDD[(Int, String)], integer: Int) = {
      test.map { case Tuple2(k, v) => Tuple1(k + integer) }.filter { case Tuple1(v) => v < 20 }
    }
  }
  test("Simple TGFs") {
    expectSql("generate shark.TestTGF1(test, 15)", Array(15, 15, 15, 17, 19).map(_.toString).toArray)
  }*/
}