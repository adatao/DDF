/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark.execution;

import static org.junit.Assert.assertEquals;
import adatao.bigr.spark.types.ExecutionResult;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import com.adatao.pa.spark.execution.LoadTable;
import com.adatao.pa.spark.execution.Sql2DataFrame;
import com.adatao.pa.spark.execution.Subset;
import com.adatao.pa.spark.execution.VectorVariance;
import com.adatao.pa.spark.execution.LoadTable.LoadTableResult;
import com.adatao.pa.spark.execution.Subset.SubsetResult;
import com.adatao.pa.spark.execution.VectorCorrelation.VectorCorrelationResult;
import com.adatao.pa.spark.execution.VectorCovariance.VectorCovarianceResult;
import com.adatao.pa.spark.execution.VectorHistogram.VectorHistogramResult;
import com.adatao.pa.spark.execution.VectorMean.VectorMeanResult;
import com.adatao.pa.spark.execution.VectorVariance.VectorVarianceResult;
import com.adatao.pa.thrift.generated.JsonCommand;
import com.adatao.pa.thrift.generated.JsonResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestVectorStats extends BaseTest {
	public static Logger LOG = LoggerFactory.getLogger(TestVectorStats.class);

	@Ignore
	public void testCSV() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		Gson gson = new Gson();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);

		try {
			LoadTable loadTbl = (LoadTable) new LoadTable()
					.setFileURL("resources/mtcars").setHasHeader(false)
					.setSeparator(" ");

			LOG.info(gson.toJson(loadTbl));
			cmd.setSid(sid).setCmdName("LoadTable")
					.setParams(gson.toJson(loadTbl));
			res = client.execJsonCommand(cmd);

			LoadTableResult result = ExecutionResult.fromJson(res.getResult(),
					LoadTableResult.class).result();
			LOG.info("LoadTable result: "
					+ Arrays.toString(result.getMetaInfo()));
			String dcID = result.getDataContainerID();

			// extract 3rd column vector
			res = client
					.execJsonCommand(cmd
							.setSid(sid)
							.setCmdName("Subset")
							.setParams(
									String.format(
											"{dataContainerID: %s,"
													+ "columns: [{type: \"Column\", index: 2}]}",
											dcID)));
			SubsetResult r = ExecutionResult.fromJson(res.getResult(),
					SubsetResult.class).result();
			String jsCreateVectors = String.format("{dataContainerID: %s}",
					r.getDataContainerID());

			// Get Mean
			cmd.setSid(sid).setCmdName("VectorMean").setParams(jsCreateVectors);
			res = client.execJsonCommand(cmd);
			VectorMeanResult gmr = ExecutionResult.fromJson(res.getResult(),
					VectorMeanResult.class).result();
			dcID = gmr.getDataContainerID();
			LOG.info("Mean Result = " + gmr.getMean());
			assertEquals((double) gmr.getMean(), 230.72, 0.01);

			// Get Variance
			cmd.setSid(sid).setCmdName("VectorVariance")
					.setParams(jsCreateVectors);
			res = client.execJsonCommand(cmd);
			VectorVarianceResult gvr = ExecutionResult.fromJson(
					res.getResult(), VectorVarianceResult.class).result();
			LOG.info("Variance Result = " + gvr.getVariance());
			assertEquals((double) gvr.getVariance(), 15360.8, 0.01);
			assertEquals((double) gvr.getStdDev(), 123.9387, 0.0001);

			// calculate histogram(mtcars$mpg)
			// extract 1rd column vector
			res = client
					.execJsonCommand(cmd
							.setSid(sid)
							.setCmdName("Subset")
							.setParams(
									String.format(
											"{dataContainerID: %s,"
													+ "columns: [{type: \"Column\", index: 0}]}",
											dcID)));
			r = ExecutionResult.fromJson(res.getResult(),
					SubsetResult.class).result();
			cmd.setSid(sid)
			.setCmdName("VectorHistogram")
			.setParams(
					String.format("{dataContainerID: %s, numBins: %s}",
							r.getDataContainerID(), 5));
			res = client.execJsonCommand(cmd);
			VectorHistogramResult vhr = ExecutionResult.fromJson(
					res.getResult(), VectorHistogramResult.class).result();
			assertEquals(true, vhr.success);
			assertEquals(5, vhr.histogramBins.size());

		} finally {
			cmd.setCmdName("disconnect").setSid(sid).setParams(null);
			client.execJsonCommand(cmd);
		}
	}

	@Ignore
	public void testFilterNA() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues()
				.create();

		JsonResult res = client.execJsonCommand(cmd);
		String sid = res.sid;
		LOG.info("Got session ID: " + sid);

		try {
			LoadTable loadTbl = (LoadTable) new LoadTable()
					.setFileURL("resources/airline.csv").setHasHeader(false)
					.setSeparator(",").setSampleSize(5);

			LOG.info(gson.toJson(loadTbl));
			cmd.setSid(sid).setCmdName("LoadTable")
					.setParams(gson.toJson(loadTbl));
			res = client.execJsonCommand(cmd);

			LoadTableResult result = ExecutionResult.fromJson(res.getResult(),
					LoadTableResult.class).result();
			LOG.info("LoadTable result: "
					+ Arrays.toString(result.getMetaInfo()));
			String dcID = result.getDataContainerID();

			// extract last column vector which has lots of NAs
			res = client
					.execJsonCommand(cmd
							.setSid(sid)
							.setCmdName("Subset")
							.setParams(
									String.format(
											"{dataContainerID: %s,"
													+ "columns: [{type: \"Column\", index: 28}]}",
											dcID)));
			SubsetResult r = ExecutionResult.fromJson(res.getResult(),
					SubsetResult.class).result();
			String jsCreateVectors = String.format("{dataContainerID: %s}",
					r.getDataContainerID());

			// Get Mean
			cmd.setSid(sid).setCmdName("VectorMean").setParams(jsCreateVectors);
			res = client.execJsonCommand(cmd);
			VectorMeanResult gmr = ExecutionResult.fromJson(res.getResult(),
					VectorMeanResult.class).result();
			assertEquals(true, gmr.isSuccess());
			assertEquals(31.33, (double) gmr.getMean(), 0.01);

			// Get Variance
			cmd.setSid(sid).setCmdName("VectorVariance")
					.setParams(jsCreateVectors);
			res = client.execJsonCommand(cmd);
			VectorVarianceResult gvr = ExecutionResult.fromJson(
					res.getResult(), VectorVariance.VectorVarianceResult.class)
					.result();
			assertEquals(535, (double) gvr.getVariance(), 0.01);
			assertEquals(23.13007, (double) gvr.getStdDev(), 0.00001);

			// calculate histogram(mtcars$mpg)
			cmd.setSid(sid)
					.setCmdName("VectorHistogram")
					.setParams(
							String.format("{dataContainerID: %s, numBins: %s}",
									dcID, 5));
			res = client.execJsonCommand(cmd);
			VectorHistogramResult vhr = ExecutionResult.fromJson(
					res.getResult(), VectorHistogramResult.class).result();
			assertEquals(true, vhr.success);
			assertEquals(3, vhr.histogramBins.size());
		} finally {
			cmd.setCmdName("disconnect").setSid(sid).setParams(null);
			client.execJsonCommand(cmd);
		}
	}

	@Ignore
	public void testShark() throws Exception {
		JsonCommand cmd = new JsonCommand().setCmdName("connect");
		// Gson gson = new Gson();
		JsonResult jres;
		String sid;

		jres = client.execJsonCommand(cmd);
		sid = jres.sid;
		LOG.info("Got session ID: " + sid);

		createTableMtcars(sid);

		try {
			Sql2DataFrame.Sql2DataFrameResult df = runSQL2RDDCmd(sid,
					"SELECT * FROM mtcars", true);

			// extract mtcars$mpg
			String jsCreateVectors = String
					.format("{columns: [{type: Column, name: mpg}], dataContainerID: %s}",
							df.dataContainerID);
			cmd.setSid(sid).setCmdName("Subset").setParams(jsCreateVectors);
			jres = client.execJsonCommand(cmd);
			SubsetResult cvr = ExecutionResult.fromJson(jres.getResult(),
					Subset.SubsetResult.class).result();
			assertEquals(true, cvr.success);
			String dcID = cvr.getDataContainerID();
			System.out.println("!!!\n!!! AHT !!! Created subset ID: " + dcID);

			// calculate mean(mtcars$mpg)
			cmd.setSid(sid).setCmdName("VectorMean")
					.setParams(String.format("{dataContainerID: %s}", dcID));
			jres = client.execJsonCommand(cmd);
			VectorMeanResult vmr = ExecutionResult.fromJson(jres.getResult(),
					VectorMeanResult.class).result();
			assertEquals(true, vmr.success);
			assertEquals(20.09062, vmr.mean, 0.00001);

			// calculate var(mtcars$mpg) and sd(mtcars$mpg)
			cmd.setSid(sid).setCmdName("VectorVariance")
					.setParams(String.format("{dataContainerID: %s}", dcID));
			jres = client.execJsonCommand(cmd);
			VectorVarianceResult vvr = ExecutionResult.fromJson(
					jres.getResult(), VectorVarianceResult.class).result();
			assertEquals(true, vvr.success);
			assertEquals(36.3241, vvr.variance, 0.0001);
			assertEquals(6.026948, vvr.stddev, 0.00001);

			// extract mtcars$hp
			jsCreateVectors = String
					.format("{columns: [{type: Column, name: hp}], dataContainerID: %s}",
							df.dataContainerID);
			cmd.setSid(sid).setCmdName("Subset").setParams(jsCreateVectors);
			jres = client.execJsonCommand(cmd);
			cvr = ExecutionResult.fromJson(jres.getResult(),
					Subset.SubsetResult.class).result();
			assertEquals(true, cvr.success);
			String hpID = cvr.getDataContainerID();
			System.out.println("!!!\n!!! AHT !!! Created subset ID: " + hpID);

			// calculate cov(mtcars$mpg, mtcars$hp)
			cmd.setSid(sid)
					.setCmdName("VectorCovariance")
					.setParams(
							String.format(
									"{xDataContainerID: %s, yDataContainerID: %s}",
									dcID, hpID));
			jres = client.execJsonCommand(cmd);
			VectorCovarianceResult vcvr = ExecutionResult.fromJson(
					jres.getResult(), VectorCovarianceResult.class).result();
			assertEquals(true, vcvr.success);
			assertEquals(-320.7321, vcvr.covariance, 0.0001);

			// calculate cor(mtcars$mpg, mtcars$hp)
			cmd.setSid(sid)
					.setCmdName("VectorCorrelation")
					.setParams(
							String.format(
									"{xDataContainerID: %s, yDataContainerID: %s}",
									dcID, hpID));
			jres = client.execJsonCommand(cmd);
			VectorCorrelationResult vcr = ExecutionResult.fromJson(
					jres.getResult(), VectorCorrelationResult.class).result();
			assertEquals(true, vcr.success);
			assertEquals(-0.7761684, vcr.correlation, 0.1);

			// calculate histogram(mtcars$mpg)
			cmd.setSid(sid)
					.setCmdName("VectorHistogram")
					.setParams(
							String.format("{dataContainerID: %s, numBins: %s}",
									dcID, 5));
			jres = client.execJsonCommand(cmd);
			VectorHistogramResult vhr = ExecutionResult.fromJson(
					jres.getResult(), VectorHistogramResult.class).result();
			assertEquals(true, vhr.success);
			assertEquals(5, vhr.histogramBins.size(), 0.00001);

		} finally {
			cmd.setCmdName("disconnect").setSid(sid).setParams(null);
			client.execJsonCommand(cmd);
		}
	}
}
