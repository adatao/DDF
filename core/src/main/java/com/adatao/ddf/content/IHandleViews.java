/**
 * 
 */
package com.adatao.ddf.content;


import java.util.List;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.ViewHandler.Column;
import com.adatao.ddf.content.ViewHandler.Expression;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 *
 */
public interface IHandleViews extends IHandleDDFFunctionalGroup {

  /**
   * @param <T>
   * @param numSamples
   * @return a new DDF containing `numSamples` rows selected randomly from our owner DDF.
   */
  public List<Object[]> getRandomSample(int numSamples, boolean withReplacement, int seed);

  public DDF getRandomSample(double percent, boolean withReplacement, int seed);

  public List<String> head(int numRows) throws DDFException;
  
  public List<String> top(int numRows, String orderCols, String mode) throws DDFException;

  public DDF project(String... columnNames) throws DDFException;
  
  public DDF project(List<String> columnNames) throws DDFException;

  public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException;
}
