package com.adatao.ddf.analytics;

import com.adatao.ddf.DDF;

/**
 * Common execution for all pAnalytics algorithms

 * @author bhan
 *
 */
public interface IRunAlgorithms {

	/**
	 * Set up parameters and data transformation for the algorithm execution
	 */
	void preprocess(DDF inputDDF);
	
	/**
	 * @param inputDDF The input DDF to be processed by the algorithm
	 * @return the {@link IAlgorithmModel}
	 */
	IAlgorithmOutputModel execute();
	void cleanup();	

}
