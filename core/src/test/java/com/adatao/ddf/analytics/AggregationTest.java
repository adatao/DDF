package com.adatao.ddf.analytics;


import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.analytics.AggregationHandler.AggregateField;

public class AggregationTest {

  @Test
  public void testAggregateSql() throws Exception {
    // aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    String fields = "year, month, min(depdelay), max(arrdelay)";
    String expectedSql = "SELECT year,month,MIN(depdelay),MAX(arrdelay) FROM airline GROUP BY year,month";
    Assert.assertEquals(expectedSql, AggregateField.toSql(AggregateField.fromSqlFieldSpecs(fields), "airline"));
  }

}
