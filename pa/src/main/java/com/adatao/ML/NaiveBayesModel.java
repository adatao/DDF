package com.adatao.ML;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.adatao.ML.TModel;

@SuppressWarnings("serial")
/**
 * Naive Bayes model that handles: 

 * TODO: We have two options here: 
 * 1. Using distribution method: 
 * Store mean m and standard deviation sd of the features in xCols[i] in featureDistribution[c][i]
 * Then for a new value v of the instance at xCols[i], 
 * compute Normal distribution : P(f(i) = v | c ) =  1/ (Math.sqrt(2*Math.PI*sd*sd) * pow(Math.E, - (v-m)*(v-m)/(2*sd*sd))
 * 2. Using discretization method:
 * Discretize the values of features in xCols[i] into K bins: 0, 1, 2,... K-1 . 
 * The value v has probability of P(f(i) = v | c) = bin(v) / K 
 * 
 * I chose to do option 1 since it require less number of variables to represent a featureDistribution[c][i]
 * @author phamnamlong
 *
 */
public class NaiveBayesModel implements TModel, Serializable {
	private double[] classPrior; // log(P(C=c))
	private double[][][] featureDistribution; // fD[c][i] contains variables that can express log(P(Fi|C=c))
	private int[] xCols;
	private int yCol;
	private int numClasses;
	private int numFeatures;

	public static Logger LOG = LoggerFactory.getLogger(NaiveBayesModel.class);

	public NaiveBayesModel(int numClasses, int numFeatures, int[] xCols, int yCol) {
		this.numClasses = numClasses;
		this.numFeatures = numFeatures;
		this.xCols = xCols.clone();
		this.yCol = yCol;
		classPrior = new double[numClasses];
		// TODO: other type of Naive Bayes model will have different number of variables to represent the feature distribution. 
		// We proceed to other type if time allowed or requested by customer.
		featureDistribution = new double[numClasses][numFeatures][2]; 
	}

	// For Binary classification ONLY, 
	// return the probability of the instance belonging to class 1.
	public double predict(Object[] row) {
		double[] logProb = new double[numClasses];
		double[] prob = new double[numClasses];
		double sumProb = 0;
		for (int c = 0; c < numClasses; c++) {
			logProb[c] = Math.log(classPrior[c]); // already computed as log
			for (int i = 0; i < xCols.length; i++) {
				double v = ((Double) row[xCols[i]]).doubleValue();
				double probD = probDistr(c, i, v);
				logProb[c] += Math.log(probD);
			}
			
			// no underflow here because we do not aggregate the products of probablities
			prob[c] = Math.pow(Math.E, logProb[c]); 
			sumProb += prob[c];
		}
		for (int c = 0; c < numClasses; c++) {
			prob[c] /= sumProb;
		}
		return prob[1];
	}
	
	private double probDistr(int c, int i, double v) {
		double m = featureDistribution[c][i][0];  // mean 
		double var = featureDistribution[c][i][1];  // variance
		double prob = 1/Math.sqrt(2*Math.PI*var) * Math.pow(Math.E, - (v-m)*(v-m)/(2*var));
		
		if (!(0 <= prob && prob <= 1)) {
			// this should never be reached!
			System.out.println("class=" + c + " feature=" + i + " mean=" + m + " var=" + var + " prob=" + prob);
		}
		return prob;
	}

	public void setPrior(int i, double v) {
		classPrior[i] = v;
	}
	
	public void setFeatureDistribution(int c, int i, List<Double> v) {
		for (int j = 0; j < v.size(); j++) {
			featureDistribution[c][i][j] = v.get(j);
		}
	}
	
	public void printAll() {
		for (int c = 0; c < numClasses; c++) {
			LOG.info("class prior: " + this.classPrior[c]);
			for (int i = 0; i < numFeatures; i++) {
				LOG.info("feature " + i + " mean=" + featureDistribution[c][i][0]);
				LOG.info("feature " + i + " var=" + featureDistribution[c][i][1]);
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("NaiveBayes[numClasses=%d numFeatures=%d]", numClasses, numFeatures);
	}

	@Override
	public Logger _LOG() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void _LOG_$eq(Logger x$1) {
		// TODO Auto-generated method stub
	}

	@Override
	public Logger LOG() {
		// TODO Auto-generated method stub
		return null;
	}

}
