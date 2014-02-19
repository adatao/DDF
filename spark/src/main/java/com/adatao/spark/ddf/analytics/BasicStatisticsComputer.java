package com.adatao.spark.ddf.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.analytics.ABasicStatisticsComputer;
import com.adatao.ddf.analytics.Summary;

public class BasicStatisticsComputer extends ABasicStatisticsComputer {
  private static final Logger LOG = LoggerFactory.getLogger(BasicStatisticsComputer.class);

  public BasicStatisticsComputer(ADDFManager theDDFManager) {
    super(theDDFManager);
  }

  @Override
  public Summary[] getSummaryImpl() {
    JavaRDD<Object[]> data = (JavaRDD) this.getManager().getRepresentationHandler().get(JavaRDD.class);
    Summary[] result = data.map(new GetSummaryMapper()).reduce(new GetSummaryReducer());
    return result;
  }

  /*
   * Mapper function
   */
  public static class GetSummaryMapper extends Function<Object[], Summary[]> {
    public GetSummaryMapper() {
    }
    @Override
    public Summary[] call(Object[] p) {
      int dim = p.length;
      if (p != null && dim > 0) {
        Summary[] result = new Summary[dim];
        for (int i = 0; i < dim; i++) {
          Summary s = new Summary();
          if (null == p[i]) {
            // LOG.debug("p = null");
            result[i] = null;
          } else {
            // LOG.debug("current p[" + j + "] : " + p[i].toString());

            if (p[i] instanceof Double) {
              Double a = (Double) p[i];
              result[i] = s.merge(a);
              // LOG.debug("after parse Double = \t" + a);

            } else if (p[i] instanceof Integer) {
              Double a = Double.parseDouble(p[i].toString());

              result[i] = s.merge(a);
              // LOG.debug("after parse Integer\t = " + "\t" + a);
            } else if (p[i] != null) {
              String current = p[i].toString();
              if (current.trim().toUpperCase().equals("NA")) {
                result[i] = new Summary();
                result[i].setIsNA(true);
                result[i].setNACount(1);
              } else {
                double test = 0.0;
                try {
                  test = Double.parseDouble(current);
                  s.merge(test);
                  result[i] = s;
                } catch (Exception ex) {
                  result[i] = null;
                  LOG.info("GetSummaryMapper: catch " + p[i] + " is not number");
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

  public static class GetSummaryReducer extends Function2<Summary[], Summary[], Summary[]> {
    public GetSummaryReducer() {
      
    }
    @Override
    public Summary[] call(Summary[] a, Summary[] b) {
      int dim = a.length;
      Summary[] result = new Summary[dim];

      for (int i = 0; i < dim; i++) {
        // LOG.debug("---------current index:" + j);
        // normal cases
        if (a[i] != null && b[i] != null) {
          if (!a[i].isNA() && !b[i].isNA()) {
            result[i] = a[i].merge(b[i]);
          } else if (!a[i].isNA()) {
            result[i] = a[i];
            result[i].addToNACount(b[i].NAcount());
          } else if (!b[i].isNA()) {
            result[i] = b[i];
            result[i].addToNACount(a[i].NAcount());
          }
          // both are NA
          else {
            // either is fine
            // do nothing
            result[i] = new Summary();
            result[i].setIsNA(true);
            result[i].setNACount(a[i].NAcount() + b[i].NAcount());
            // LOG.debug("Reducer:---------------  Got NAs: number of NA = 2");
          }
        } else {

          if (a[i] != null) {
            result[i] = new Summary();
            result[i] = a[i];
            result[i].addToNACount(1);
            // LOG.debug("Reducer:--------------- Got b = null, a!= null, a="+ a[i].sum());
          } else if (b[i] != null) {
            result[i] = new Summary();
            result[i] = b[i];
            result[i].addToNACount(1);
            // LOG.debug("Reducer:--------------- Got a = null, b!= null, b="+ b[i].sum());
          }
          // both are null
          else {
            // LOG.debug("Reducer:--------------- Got null, both are null");
          }
        }
        LOG.debug("---------------");
      }
      return result;
    }
  }
}
