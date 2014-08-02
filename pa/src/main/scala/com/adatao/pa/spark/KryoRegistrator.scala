package com.adatao.pa.spark

import com.esotericsoftware.kryo.Kryo
import shark.{KryoRegistrator => SharkKryoRegistrator}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, FieldSerializer}
import io.ddf.types.Matrix
import io.ddf.types.Vector
import com.adatao.spark.ddf.analytics.ALossFunction
import com.adatao.pa.spark.execution.LinearRegression
import com.adatao.pa.spark.execution.LogisticRegression
import com.adatao.pa.ML.types.randomforest.node.{Leaf, NumericalNode, CategoricalNode, Node}
import com.adatao.spark.ddf.analytics.LinearRegressionModel
import com.adatao.spark.ddf.analytics.LogisticRegressionModel
import org.rosuda.REngine.{RList, REXP}
import org.jblas.DoubleMatrix

class KryoRegistrator extends SharkKryoRegistrator {
	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[Vector])
		kryo.register(classOf[Matrix])
		kryo.register(classOf[ALossFunction])
		kryo.register(classOf[DoubleMatrix])
//		kryo.register(classOf[LinearRegression.LossFunction],
//			new FieldSerializer[Nothing](kryo, classOf[LinearRegression.LossFunction]))
		kryo.register(classOf[LogisticRegression.LossFunction])
		kryo.register(classOf[NumericalNode])
		kryo.register(classOf[CategoricalNode])
		kryo.register(classOf[Leaf])
		kryo.register(classOf[Node])
		kryo.register(classOf[org.apache.spark.mllib.clustering.KMeansModel])
		kryo.register(classOf[LinearRegressionModel])
		kryo.register(classOf[LogisticRegressionModel])
		kryo.register(classOf[REXP])
		kryo.register(classOf[RList], new FieldSerializer(kryo, classOf[RList]))
		super.registerClasses(kryo)
	}
}
