package adatao.bigr.spark

import com.esotericsoftware.kryo.Kryo
import shark.{KryoRegistrator => SharkKryoRegistrator}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, FieldSerializer}
import adatao.ML.types.{Vector, Matrix}
import adatao.ML.ALossFunction
import adatao.bigr.spark.execution.LinearRegression
import adatao.bigr.spark.execution.LogisticRegression
import adatao.ML.spark.RocObject
import adatao.ML.types.randomforest.node.{Leaf, NumericalNode, CategoricalNode, Node}
import adatao.ML.KmeansModel
import adatao.ML.LinearRegressionModel
import adatao.ML.LogisticRegressionModel
import org.rosuda.REngine.{RList, REXP}
import org.jblas.DoubleMatrix

class KryoRegistrator extends SharkKryoRegistrator {
	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[Vector])
		kryo.register(classOf[Matrix])
		kryo.register(classOf[ALossFunction])
		kryo.register(classOf[DoubleMatrix])
		kryo.register(classOf[LinearRegression.LossFunction],
			new FieldSerializer[Nothing](kryo, classOf[LinearRegression.LossFunction]))
		kryo.register(classOf[LogisticRegression.LossFunction])
		kryo.register(classOf[RocObject])
		kryo.register(classOf[NumericalNode])
		kryo.register(classOf[CategoricalNode])
		kryo.register(classOf[Leaf])
		kryo.register(classOf[Node])
		kryo.register(classOf[KmeansModel])
		kryo.register(classOf[LinearRegressionModel])
		kryo.register(classOf[LogisticRegressionModel])
		kryo.register(classOf[REXP])
		kryo.register(classOf[RList], new FieldSerializer(kryo, classOf[RList]))
		super.registerClasses(kryo)
	}
}