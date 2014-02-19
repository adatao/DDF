package com.adatao.ddf.content;

import com.adatao.ddf.DDF;

/**
 * author: daoduchuan
 */
public interface IFactorSupporter {
  /**
   *
   * @param columnID
   * @return
   */
  public AFactorSupporter.FactorColumnInfo factorize(int columnID);

  /**
   *
   * @param columnIDs
   * @return
   */
  public AFactorSupporter.FactorColumnInfo[] factorize(int[] columnIDs);

  /**
   *
   * @param col
   * @param ddf
   * @return
   */
  public DDF applyFactorCoding(int col, DDF ddf);

  /**
   *
   * @param cols
   * @param ddf
   * @return
   */
  public DDF applyFactorCoding(int[] cols, DDF ddf);

}
