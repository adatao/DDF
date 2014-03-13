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

import java.io.Serializable;
import java.text.DecimalFormat;
import adatao.ML.types.TJsonSerializable;
import adatao.ML.types.TJsonSerializable$class;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import adatao.ML.Utils;

/**
 * @author bachbui Implement summary function for both vector and dataframe
 */
@SuppressWarnings("serial")
public class QuickSummary extends CExecutor {
  private String dataContainerID;
  DataManager dm;

  public static Logger LOG = LoggerFactory.getLogger(QuickSummary.class);

	public static class DataframeStatsResult extends SuccessResult implements TJsonSerializable, Serializable {

    String dataContainerID;
    // StatCounter
    public double[] mean;
    public double[] sum;
    public double[] stdev;
    public double[] var;
    public int[] cNA;
    public long[] count;
    public double[] min;
    public double[] max;
    String clazz;


    public String clazz() {
      return clazz;
    }

		public void adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(java.lang.String Aclass) {
			clazz = Aclass;
		}


		public TJsonSerializable fromJson(String jsonString) {
			return TJsonSerializable$class.fromJson(this, jsonString);
		}

    public void setStats(StatCounterExt[] stats) {
      int dim = stats.length;
      mean = new double[dim];
      sum = new double[dim];
      stdev = new double[dim];
      var = new double[dim];
      cNA = new int[dim];
      count = new long[dim];

      min = new double[dim];
      max = new double[dim];

      // by default is x.xx
      DecimalFormat df = new DecimalFormat("#.##");

      for (int i = 0; i < stats.length; i++) {
        if (stats[i] != null) {
          mean[i] = Utils.toJavaDouble(df.format(stats[i].mean()));
          sum[i] = Utils.toJavaDouble(df.format(stats[i].sum()));
          stdev[i] = Utils.toJavaDouble(df.format(stats[i].sampleStdev()));
          var[i] = Utils.toJavaDouble(df.format(stats[i].variance()));
          cNA[i] = stats[i].cNA();
          count[i] = stats[i].count();

          min[i] = Utils.toJavaDouble(df.format(stats[i].min()));
          max[i] = Utils.toJavaDouble(df.format(stats[i].max()));

          // LOG.info(mean[i] + ";" + sum[i] + ";" + stdev[i] + ";"
          // + var[i] + ";" + cNA[i] + ";" + count[i] + ";"
          // + min[i] + ";" + max[i]);
        }
      }
    }

    public String getDataContainerID() {
      return dataContainerID;
    }

    public DataframeStatsResult setDataContainerID(String dataContainerID) {
      this.dataContainerID = dataContainerID;
			this.adatao$ML$types$TJsonSerializable$_setter_$clazz_$eq(this.getClass().getName());
      return this;
    }
  }

  public static class StatCounterExt extends StatCounter {
    int countNA = 0;

    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    boolean isNA = false;
    boolean ignore = false;

    public StatCounterExt merge(double values) {
      if (values == Double.NaN) countNA++;

      this.min = Math.min(this.min, values);
      this.max = Math.max(this.max, values);

      super.merge(values);
      return this;
    }

    public StatCounterExt merge(StatCounterExt other) {
      StatCounterExt a = (StatCounterExt) super.merge(other);
      a.countNA = other.countNA + this.countNA;

      a.min = Math.min(a.min, other.min);
      a.max = Math.max(a.max, other.max);

      return a;
    }

    public int cNA() {
      return countNA;
    }

    public double min() {
      return min;
    }

    public double max() {
      return max;
    }
  }

  /*
   * Mapper function
   */
  static class GetMeanMapper extends Function<Object[], StatCounterExt[]> {
    public GetMeanMapper() {
    }

    public StatCounterExt[] call(Object[] p) {
      int dim = p.length;
      // LOG.debug("dimension = " + dim);
      if (p != null && dim > 0) {
        StatCounterExt[] result = new StatCounterExt[dim];
        for (int j = 0; j < dim; j++) {
          StatCounterExt s = new StatCounterExt();
          // LOG.debug("j=" + j);
          if (null == p[j]) {
            // LOG.debug("p = null");
            result[j] = null;
          } else {
            // LOG.debug("current p[" + j + "] : " + p[j].toString());

            if (p[j] instanceof Double) {
              Double a = (Double) p[j];
              result[j] = s.merge(a);
              // LOG.debug("after paarse Double = \t" + a);

            } else if (p[j] instanceof Integer) {
              Double a = Utils.toJavaDouble(p[j].toString());

              result[j] = s.merge(a);
              // LOG.debug("after parse Integer\t = " + "\t" + a);
            } else if (p[j] != null) {
              // dont merge
              // do nothing
              // would be NA
              String current = p[j].toString();
              if (current.trim().toLowerCase().equals("na")) {
                result[j] = new StatCounterExt();
                result[j].isNA = true;
                result[j].countNA = 1;
                // LOG.debug("GetMeanMapper: catch NA \t");
              } else {
                double test = 0.0;
                try {
                  test = Utils.toJavaDouble(current);
                  s.merge(test);
                  result[j] = s;
                  // LOG.debug("catch \t" + test);
                } catch (Exception ex) {
                  result[j] = null;
                  // LOG.debug("GetMeanMapper: catch " + p[j] + " is not number");
                }
              }
            }
          }

        }
        return result;
      } else {
        LOG.error("malformed line input");
        return null;
      }
    }
  }

  static class GetMeanReducer extends Function2<StatCounterExt[], StatCounterExt[], StatCounterExt[]> {
    public StatCounterExt[] call(StatCounterExt[] a, StatCounterExt[] b) {
      int dim = a.length;
      StatCounterExt[] result = new StatCounterExt[dim];

      for (int j = 0; j < dim; j++) {
        // LOG.debug("---------current index:" + j);
        // normal cases
        if (a[j] != null && b[j] != null) {
          if (!a[j].isNA && !b[j].isNA) {
            result[j] = a[j].merge(b[j]);
          } else if (!a[j].isNA) {
            result[j] = a[j];
            result[j].countNA += b[j].countNA;
            // LOG.debug("Reducer:--------------- Got NAs: number of NA = 1");
          } else if (!b[j].isNA) {
            result[j] = b[j];
            result[j].countNA += a[j].countNA;// + 1;
            // LOG.debug("Reducer:--------------- Got NAs: number of NA = 1");
          }
          // both are NAs
          else {
            // either is fine
            // do nothing
            result[j] = new StatCounterExt();
            result[j].isNA = true;
            result[j].countNA += b[j].countNA + a[j].countNA;
            // LOG.debug("Reducer:---------------  Got NAs: number of NA = 2");
          }
        } else {
          // added June 6th, Reducer doest get null from Mapper
          if (a[j] != null) {
            result[j] = new StatCounterExt();
            // result[j].isNA = true;
            result[j] = a[j];
            result[j].countNA += 1;
            // LOG.debug("Reducer:--------------- Got b = null, a!= null, a="+ a[j].sum());
          } else if (b[j] != null) {
            result[j] = new StatCounterExt();
            // result[j].isNA = true;
            result[j] = b[j];
            result[j].countNA += 1;
            // LOG.debug("Reducer:--------------- Got a = null, b!= null, b="+ b[j].sum());
          }
          // both are null ?
          else {
            // LOG.debug("Reducer:--------------- Got null, both are null");
          }
        }
        LOG.debug("---------------");
      }
      return result;
    }
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) {
    // first get dataframe
    dm = sparkThread.getDataManager();
      DataManager.DataContainer df = dm.get(dataContainerID);
		DataframeStatsResult dfs = df.getQuickSummary();

    if (dfs == null) {
      StatCounterExt[] res = df.getRDD().map(new GetMeanMapper()).reduce(new GetMeanReducer());

			dfs = new DataframeStatsResult();
      dfs.setStats(res);
      dfs.setDataContainerID(dataContainerID);
      df.putQuickSummary(dfs);
      return dfs;
    } else {
      LOG.info("QuickSummary cache hit !!!");
      return dfs;
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public QuickSummary setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
