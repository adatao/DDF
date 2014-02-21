package com.adatao.ddf.analytics;

import java.io.Serializable;

import com.adatao.ddf.util.DDFUtils;

/**
 * Basic statistics for a set of double numbers including min, max, count,
 * NAcount, mean, variance and stdev
 * 
 * @author bhan
 * 
 */
@SuppressWarnings("serial")
public class Summary implements Serializable {

  private long mCount = 0; // tracking number of non-NA values
  private double mMean = 0; // tracking mean
  private double tmpVar = 0; // tracking variance
  private long mNACount = 0; // tracking number of NA values
  private double mMin = Double.MAX_VALUE;
  private double mMax = Double.MIN_VALUE;

  public Summary() {
  }

  public Summary(double[] numbers) {
    this.merge(numbers);
  }

  public Summary(long mCount, double mMean, double tmpVar, long mNACount,
      double mMin, double mMax) {
    super();
    this.mCount = mCount;
    this.mMean = mMean;
    this.tmpVar = tmpVar;
    this.mNACount = mNACount;
    this.mMin = mMin;
    this.mMax = mMax;
  }

  public Summary newSummary(Summary a) {
    return new Summary(a.mCount, a.mMean, a.tmpVar, a.mNACount, a.mMin, a.mMax);
  }

  public long count() {
    return mCount;
  }

  public long NACount() {
    return this.mNACount;
  }

  public double mean() {
    if (mCount == 0)
      return Double.NaN;
    return this.mMean;
  }

  public double tmpVar() {
    return this.tmpVar;
  }

  public boolean isNA() {
    return (mCount == 0 && mNACount > 0);
  }

  public void setNACount(long n) {
    this.mNACount = n;
  }

  public double min() {
    if (mCount == 0)
      return Double.NaN;

    return mMin;
  }

  public double max() {
    if (mCount == 0)
      return Double.NaN;
    return mMax;
  }

  public void addToNACount(long number) {
    this.mNACount += number;
  }

  public Summary merge(double number) {
    if (Double.isNaN(number)) {
      this.mNACount++;
    } else {

      this.mCount++;

      double delta = number - mMean;
      mMean += delta / mCount;
      tmpVar += delta * (number - mMean);
      mMin = Math.min(mMin, number);
      mMax = Math.max(mMax, number);
    }
    return this;
  }

  public Summary merge(double[] numbers) {
    for (double number : numbers) {
      this.merge(number);
    }
    return this;
  }

  public Summary merge(Summary other) {
    if (this.equals(other)) {
      return merge(newSummary(other));// for self merge
    } else {
      if (mCount == 0) {
        mMean = other.mean();
        tmpVar = other.tmpVar();
        mCount = other.count();
      } else if (other.mCount != 0) {
        double delta = other.mean() - mMean;
        long n = (mCount + other.mCount);
        if (other.mCount * 10 < mCount) {
          mMean = mMean + (delta * other.mCount) / n;
        } else if (mMean * 10 < other.mean()) {
          mMean = other.mean() - (delta * mCount) / n;
        } else {
          mMean = (mMean * mCount + other.mean() * other.mCount) / n;
        }
        tmpVar += other.tmpVar() + (delta * delta * mCount * other.mCount) / n;
        mCount += other.mCount;
      }
      this.mNACount += other.NACount();
      this.mMin = Math.min(this.mMin, other.mMin);
      this.mMax = Math.max(this.mMax, other.mMax);
      return this;
    }

  }

  public double sum() {
    return DDFUtils.formatDouble(mMean * mCount);
  }

  public double stdev() {

    return DDFUtils.formatDouble(Math.sqrt(variance()));

  }

  public double variance() {
    if (mCount <= 1) {
      return Double.NaN;
    } else {
      return DDFUtils.formatDouble(tmpVar / (mCount - 1));
    }
  }

  @Override
  public String toString() {
    // return String.format(
    // "mean:%f stdev:%f var:%f cNA:%d count:%d min:%f max:%f", mean(),
    // stdev(), variance(), mNACount, mCount, min(), max());
    return "mean:" + mean() + " stdev:" + stdev() + " var:" + variance()
        + " cNA:" + mNACount + " count:" + mCount + " min:" + min() + " max:"
        + max();

  }
}