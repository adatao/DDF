package com.adatao.ML


/**
 * Created with IntelliJ IDEA.
 * User: daoduchuan
 * Date: 6/8/13
 * Time: 4:39 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class AUnsupervisedModel[InputType, OutputType](val centroids: java.util.List[Array[Double]], val numSamples: Long)
	extends TPredictiveModel[InputType, OutputType] {
	/*
	*/
	def predict(points: InputType): OutputType

}

