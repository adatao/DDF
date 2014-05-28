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



/**
 * @author ngonpham Get mean for both vector and dataframe
 */
@SuppressWarnings("serial")
public class VectorMean { // implements IExecutor, Serializable {

  private String dataContainerID;


  public VectorMean setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

  // public static Logger LOG = LoggerFactory.getLogger(VectorMean.class);
  //
  // static public class VectorMeanResult extends SuccessResult implements TJsonSerializable {
  // String dataContainerID;
  // double mean;
  // String clazz;
  //
  // public String getDataContainerID() {
  // return dataContainerID;
  // }
  //
  // public VectorMeanResult setDataContainerID(String dataContainerID) {
  // this.dataContainerID = dataContainerID;
  // return this;
  // }
  //
  // public VectorMeanResult setMean(double mean) {
  // this.mean = mean;
  // return this;
  // }
  //
  // public double getMean() {
  // return mean;
  // }
  //
  // public String clazz() {
  // return clazz;
  // }
  //
  // public void com$adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
  // clazz = Aclass;
  // }
  //
  // @Override
  // public TJsonSerializable fromJson(String jsonString) {
  // return TJsonSerializable$class.fromJson(this, jsonString);
  // }
  // }
  //
  // @Override
  // public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
  // DataContainer dc = sparkThread.getDataManager().get(dataContainerID);
  // if (dc.getType().equals(DataContainer.ContainerType.DataFrame)) {
  // DataFrame df = (DataFrame) sparkThread .getDataManager().get(dataContainerID);
  // // System.out.println("First = " + df.getRDD().first()[0]);
  // JavaRDD<Object[]> filteredRdd = df.getRDD().filter(
  // new Function<Object[], Boolean>() {
  // @Override
  // public Boolean call(Object[] t) throws Exception {
  // // System.out.println(t[0]);
  // return (t[0] != null)
  // && ((t[0] instanceof Integer) || (!Double
  // .isNaN((Double) t[0])));
  // }
  // });
  // JavaDoubleRDD rdd = filteredRdd.map(new DoubleFunction<Object[]>() {
  // @Override
  // public Double call(Object[] t) throws Exception {
  // if (t[0] instanceof Integer)
  // return (double) ((Integer) t[0]).intValue();
  // else
  // return (Double) t[0];
  // }
  // });
  // StatCounter stat = rdd.stats();
  // double mean = stat.mean();
  // return new VectorMeanResult().setDataContainerID(dataContainerID).setMean( mean);
  // } else if (dc.getType().equals(DataContainer.ContainerType.SharkColumnVector)) {
  // SharkColumnVector v = (SharkColumnVector) dc;
  // JavaSharkContext sc = (JavaSharkContext) sparkThread.getSparkContext();
  //
  // Double mean = SharkQueryUtils.sql2Double(sc, String.format("select avg(%s) from %s", v.getColumn(), v.tableName),
  // AdataoExceptionCode.ERR_GENERAL);
  // return new VectorMeanResult().setDataContainerID(dataContainerID).setMean(mean);
  // } else {
  // throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_CONTAINER_TYPE,
  // String.format("the mean function argument must be a vector: found type %s", dc.getType()), null);
  // }
  // }
}
