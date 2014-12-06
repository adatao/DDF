package com.adatao.pa.spark.execution

import com.adatao.pa.spark.types.ABigRClientTest
import com.adatao.pa.spark.execution.FetchRows.FetchRowsResult
import scala.collection.JavaConversions._
import com.adatao.pa.spark.Utils.DataFrameResult
import org.scalatest.BeforeAndAfterAll

// @formatter:off

/**
 * @author aht
 */
class MapReduceNativeSuite extends ABigRClientTest {
//	override def beforeAll = {
//		createTableMtcars
//		createTableAirline
//		createTableAirQuality
//	}


	test("aggregate(hp ~ gear, mtcars, FUN=sum), several unique keys reduce, vector map key, vector map value") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		// aggregate sum of hp group by gear
		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(key=part$gear, val=part$hp) }",
			"function(key, vv) { keyval.row(key=key, val=sum(vv)) }", true)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "val"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "int"))
//
		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
//
		//aggregate(hp ~ gear, mtcars, FUN=sum)
		// [["4",1074],["3",2642],["5",978]]
		val res = r2.result.data.map { row => (row(0), row(1)) }.toMap
		assert(res === Map("3" -> 2642, "4" -> 1074, "5" -> 978))
	}

	test("aggregate(hp ~ gear, mtcars, FUN=sum), several unique keys reduce, vector map key, vector map value, mapside.combine = false") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		// aggregate sum of hp group by gear
		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(key=part$gear, val=part$hp) }",
			"function(key, vv) { keyval.row(key=key, val=sum(vv)) }",
		  false
		)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "val"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "int"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		// aggregate(hp ~ gear, mtcars, FUN=sum)
		// [["4",1074],["3",2642],["5",978]]
		val res = r2.result.data.map { row => (row(0), row(1)) }.toMap
		assert(res === Map("3" -> 2642, "4" -> 1074, "5" -> 978))
	}

	test("sum(mtcars$wt) and sum(mtcars$hp), global reduce, vector map key, data.frame map value") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		// calculate sum of mtcars$wt
		// global reduce with key
		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(key=rep('global', nrow(part)), val=part[, c('wt', 'hp')]) }",
			"function(key, vv) { print(vv); keyval.row(key=key, val=list(wt=sum(vv$wt), hp=sum(vv$hp))) }", true)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "wt", "hp"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "double", "int"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		assert(r2.result.data.get(0)(1) === 102.952) // sum(mtcars$wt)
		assert(r2.result.data.get(0)(2) === 4694.0)  // sum(mtcars$hp)
	}

	test("sum(mtcars$wt) and sum(mtcars$hp), global reduce, vector map key, data.frame map value, false") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		// calculate sum of mtcars$wt
		// global reduce with key
		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(key=rep('global', nrow(part)), val=part[, c('wt', 'hp')]) }",
			"function(key, vv) { keyval.row(key=key, val=list(wt=sum(vv$wt), hp=sum(vv$hp))) }",
			false
		)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "wt", "hp"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "double", "int"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		assert(r2.result.data.get(0)(1) === 102.952) // sum(mtcars$wt)
		assert(r2.result.data.get(0)(2) === 4694.0)  // sum(mtcars$hp)

	}

	ignore("aggregate(solar_radiation ~ month, airquality, mean) with NA handing using R mean(), false") {
    createTableAirQuality
		val loader = new Sql2DataFrame("select * from airquality", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val mr = new MapReduceNative(dataContainerId,
			"function(part) { " +
					"part <- part[ !is.na(part$solar_radiation), ]; " +
					"keyval(key=part$month, val=part$solar_radiation) }",
			"function(key, vv) { keyval.row(key, mean(vv$val1)) }",
			false)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "val"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "double"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
//
		// aggregate(Solar.R ~ Month, airquality, mean)
		val res = r2.result.data.map { row => (row(0), row(1).asInstanceOf[Double]) }.toMap
		val expected = Map("5" -> 181.2963, "6" -> 190.1667, "7" -> 216.4839, "8" -> 171.8571, "9" -> 167.4333)
		res.forall { case (k, v) => math.abs(v - expected.get(k)) < 0.01 }
	}

	test("aggregate(solar_radiation ~ month, airquality, mean) with NA handing using sum & count, false") {
    createTableAirQuality
		val loader = new Sql2DataFrame("select * from airquality", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val mr = new MapReduceNative(dataContainerId,
			"function(part) { " +
					"part <- part[ !is.na(part$solar_radiation), ]; " +
					"keyval(key=part$month, val=data.frame(sum=part$solar_radiation, count=rep(1, nrow(part)))) }",
			"function(key, vv) { keyval.row(key, list(sum=sum(vv$sum), count=sum(vv$count))) }",
			false)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "sum", "count"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "int", "double"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
//
//		// aggregate(Solar.R ~ Month, airquality, mean)
		val res = r2.result.data.map { row => (row(0), row(1).asInstanceOf[Double] / row(2).asInstanceOf[Double]) }.toMap
		val expected = Map("5" -> 181.2963, "6" -> 190.1667, "7" -> 216.4839, "8" -> 171.8571, "9" -> 167.4333)
		res.forall { case (k, v) => math.abs(v - expected.get(k)) < 0.01 }
	}


	test("aggregate(solar_radiation ~ month, airquality, mean) with NA handing  using sum & count, true") {
    createTableAirQuality
		val loader = new Sql2DataFrame("select * from airquality", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val mr = new MapReduceNative(dataContainerId,
			"function(part) { " +
					"part <- part[ !is.na(part$solar_radiation), ]; " +
					"keyval(key=part$month, val=data.frame(sum=part$solar_radiation, count=rep(1, nrow(part)))) }",
			"function(key, vv) { keyval.row(key, list(sum=sum(vv$sum), count=sum(vv$count))) }",
			true)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "sum", "count"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "int", "double"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		// aggregate(Solar.R ~ Month, airquality, mean)
		val res = r2.result.data.map { row => (row(0), row(1).asInstanceOf[Double] / row(2).asInstanceOf[Double]) }.toMap
		val expected = Map("5" -> 181.2963, "6" -> 190.1667, "7" -> 216.4839, "8" -> 171.8571, "9" -> 167.4333)
		res.forall { case (k, v) => math.abs(v - expected.get(k)) < 0.01 }
	}

	test("heterogenous map values") {
    createTableAirline
		val loader = new Sql2DataFrame("select * from airline", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val mr = new MapReduceNative(dataContainerId,
			"function(part) { " +
					"keyval(key=part[, 2], val=part[, c(14, 16, 8)]) }", // key = month, val = arrdelay, origin, unique carrier
			"function(k, vv) {" +
					"keyval.row(k, val=list(arrdelay=mean(vv[, 1])," +
					"                       origin_count=length(levels(factor(vv[, 2]))))) }",
			false)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "arrdelay", "origin_count"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "double", "int"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
	}

	ignore("multiple reduce output per key") {
    createTableMtcars
		val loader = new Sql2DataFrame("select * from mtcars", true)
		val r0 = bigRClient.execute[Sql2DataFrame.Sql2DataFrameResult](loader).result
		assert(r0.isSuccess)

		val dataContainerId = r0.dataContainerID

		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(part$gear, part) }",
			"function(k, vv) {  df <- vv; df$row_count <- nrow(vv); keyval(key=rep(k, nrow(vv)), val=df) }",
			false)
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader).length === 13)

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)
	}
	/*
	test("can mapreduce a Object[] DataFrame") {
		val dataContainerId = this.loadFile(List("resources/mtcars", "server/resources/mtcars"), false, " ")

		// calculate sum of mtcars$wt
		// global reduce with key
		val mr = new MapReduceNative(dataContainerId,
			"function(part) { keyval(key=rep('global', nrow(part)), val=part[, c('V4', 'V6')]) }",
			"function(key, vv) { print(vv); keyval.row(key=key, val=list(V4=sum(vv$V4), V6=sum(vv$V6))) }")
		val r1 = bigRClient.execute[DataFrameResult](mr)
		assert(r1.isSuccess)

		assert(r1.result.metaInfo.map(_.getHeader) === Array("key", "V4", "V6"))
		assert(r1.result.metaInfo.map(_.getType) === Array("string", "double", "double"))

		val fetcher = new FetchRows().setDataContainerID(r1.result.dataContainerID).setLimit(10)
		val r2 = bigRClient.execute[FetchRowsResult](fetcher)
		assert(r2.isSuccess)

		assert(r2.result.data.get(0)(1) === 4694.0)  // sum(mtcars$hp)
		assert(r2.result.data.get(0)(2) === 102.952) // sum(mtcars$wt)
	}   */
}
