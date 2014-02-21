package com.adatao.spark.ddf.analytics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.analytics.ABasicStatisticsComputer;
import com.adatao.ddf.analytics.Summary;
/**
 * Compute the basic statistics for each column in a RDD-based DDF
 * @author bhan
 *
 */
public class BasicStatisticsComputer extends ABasicStatisticsComputer {
  private static final Logger sLOG = LoggerFactory
      .getLogger(BasicStatisticsComputer.class);

  public BasicStatisticsComputer(ADDFManager theDDFManager) {
    super(theDDFManager);
  }

  @Override
  public Summary[] getSummaryImpl() {
    @SuppressWarnings("unchecked")
    JavaRDD<Object[]> data = (JavaRDD<Object[]>) this.getManager().getRepresentationHandler().get(Object[].class);
    Summary[] stats = data.map(new GetSummaryMapper()).reduce(
        new GetSummaryReducer());
    return stats;
  }

  /*
   * Mapper function
   */
  public static class GetSummaryMapper extends Function<Object[], Summary[]> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
            result[i] = null;
          } else {
            if (p[i] instanceof Double) {
              Double a = (Double) p[i];
              result[i] = s.merge(a);
            } else if (p[i] instanceof Integer) {
              Double a = Double.parseDouble(p[i].toString());
              result[i] = s.merge(a);
            } else if (p[i] != null) {
              String str = p[i].toString();
              if (str.trim().toUpperCase().equals("NA")) {
                result[i] = new Summary();
                result[i].setNACount(1);
              } else {
                double number = 0.0;
                try {
                  number = Double.parseDouble(str);
                  s.merge(number);
                  result[i] = s;
                } catch (Exception ex) {
                  result[i] = null;
                  sLOG.info("GetSummaryMapper: catch " + p[i] + " is not number");
                }
              }
            }
          }
        }
        return result;
      } else {
        sLOG.error("malformed line input");
        return null;
      }
    }
  }

  public static class GetSummaryReducer extends
      Function2<Summary[], Summary[], Summary[]> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public GetSummaryReducer() {

    }

    @Override
    public Summary[] call(Summary[] a, Summary[] b) {
      int dim = a.length;
      Summary[] result = new Summary[dim];

      for (int i = 0; i < dim; i++) {
        // normal cases
        if (a[i] != null && b[i] != null) {
          if (!a[i].isNA() && !b[i].isNA()) {
            result[i] = a[i].merge(b[i]);
          } else if (!a[i].isNA()) {
            result[i] = a[i];
            result[i].addToNACount(b[i].NACount());
          } else if (!b[i].isNA()) {
            result[i] = b[i];
            result[i].addToNACount(a[i].NACount());
          }
          // both are NA
          else {
            result[i] = new Summary();
            result[i].setNACount(a[i].NACount() + b[i].NACount());
          }
        } else {
          if (a[i] != null) {
            result[i] = new Summary();
            result[i] = a[i];
            result[i].addToNACount(1);
          } else if (b[i] != null) {
            result[i] = new Summary();
            result[i] = b[i];
            result[i].addToNACount(1);
          }
        }
      }
      return result;
    }
  }
}
