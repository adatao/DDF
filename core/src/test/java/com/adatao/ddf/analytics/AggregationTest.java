package com.adatao.ddf.analytics;


import static org.junit.Assert.assertTrue;
import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.types.AggregateTypes.AggregateField;
import com.adatao.ddf.facades.RFacade;
public class AggregationTest {

  @Test
  public void testAggregateSql() throws Exception {
    // aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    String fields = "year, month, min(depdelay), max(arrdelay)";
    String expectedSql = "SELECT year,month,MIN(depdelay),MAX(arrdelay) FROM airline GROUP BY year,month";
    Assert.assertEquals(expectedSql, AggregateField.toSql(AggregateField.fromSqlFieldSpecs(fields), "airline"));
  }

  @Test
  public void testRAggregateFormular() {
    
    String rAggregateFormula = "cbind(mpg,hp) ~ vs + am, mtcars, FUN=mean";
    assertTrue(rAggregateFormula.matches("^\\s*cbind\\((.+)\\)\\s*~\\s*(.+),(.+),(.+)"));    

    Assert.assertEquals("vs,am,mean(mpg),mean(hp)", RFacade.parseRAggregateFormula(rAggregateFormula));    
  }
}
