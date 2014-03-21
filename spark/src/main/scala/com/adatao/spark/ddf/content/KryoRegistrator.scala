package com.adatao.spark.content

import com.esotericsoftware.kryo.Kryo
import shark.{KryoRegistrator => SharkKryoRegistrator}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, FieldSerializer}
import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.spark.ddf.analytics._
import com.adatao.spark.ddf.content._
import org.jblas.DoubleMatrix

class KryoRegistrator extends SharkKryoRegistrator {
	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[Vector])
		kryo.register(classOf[Matrix])
		kryo.register(classOf[ALossFunction])
		kryo.register(classOf[DoubleMatrix])
		kryo.register(classOf[LossFunction],
			new FieldSerializer[Nothing](kryo, classOf[LossFunction]))
		kryo.register(classOf[LogisticRegressionModel])
		super.registerClasses(kryo)
	}
}
