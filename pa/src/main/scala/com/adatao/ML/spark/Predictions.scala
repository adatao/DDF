package com.adatao.ML.spark

import com.adatao.ML._
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import org.apache.spark.rdd.RDD
import com.adatao.pa.spark.DataManager.SharkDataFrame
import com.adatao.pa.spark.DataManager.DataContainer
import com.adatao.ML.Kmeans.{ ParsePoint, DataPoint }
import java.lang.Integer

/**
 */
object Predictions {

  // batch prediction on a feature-extracted RDD[(Matrix, Vector)]
  def yTrueYpred[T <: TPredictiveModel[Vector, Double]](model: T, xyRDD: RDD[(Matrix, Vector)]): RDD[(Double, Double)] = {
    xyRDD.flatMap { xy ⇒
      xy match {
        case (x, y) ⇒ for (i ← 0 until y.size) yield (y(i), model.predict(Vector(x.getRow(i))))
      }
    }
  }

  // batch prediction on a SharkDataframe and DataFrame,
  // in which case:
  //   - we have no type-safe knowledge about how model performs prediction
  //   - we allow some model to perform pre-processing e.g. convert String to Double
  def yTrueYpred[T <: TModel](
    model: T,
    dataframe: DataContainer,
    xCols: Array[Int],
    yCol: Int): RDD[(Double, Double)] = {

	  // rdd contains null-filtered instances 
      val rdd = dataframe.getRDD().rdd.filter { row =>
        row(yCol) != null && xCols.map(i => row(i)).forall(x => x!= null)
      }

    model match {
      case m: ALinearModel[_] ⇒ {
        var trDummyCoding = new TransformDummyCoding(model, xCols)
        var (dataPartition, dcm) = trDummyCoding.transform(dataframe, yCol)

        Predictions.yTrueYpred(m.asInstanceOf[ALinearModel[Double]], dataPartition)
      }
      case m: RandomForestModel ⇒ {                
        // cannot use getDataTable, we must go throw object boxing here
        if (m.dataFormat.isRegression) {
          rdd.map { row ⇒
            var obj = row(yCol)
            var yTrue: Double = Double.NaN;
            if (obj.isInstanceOf[Integer])
              yTrue = obj.asInstanceOf[Integer].doubleValue();
            else 
              yTrue = obj.asInstanceOf[Double];
            (yTrue, m.predict(row))
          }
        } else {
          // map categorical yCol to Double
          val map = m.dataFormat.getMapValues(m.dataFormat.getLabelId());
          rdd.map { row ⇒
            var trueVal: String = null
            var mapGet: Int = -1
            var predVal: Double = Double.NaN
            var mapGetToDouble: Double = Double.NaN
            try {
              trueVal = row(yCol).toString()
              mapGet = map.get(trueVal)
              predVal = m.predict(row)
              mapGetToDouble = mapGet.toDouble
            } catch {
              case npe: NullPointerException => {
                println(trueVal, mapGet, predVal)
              }
            }
            
            (mapGetToDouble, predVal)
          }
        }
      }
      case m: NaiveBayesModel => {
        rdd.map {
          row => (row(yCol).asInstanceOf[Double], m.predict(row));
        }
      }
      
      case _ ⇒ throw new IllegalArgumentException("don't know how to predict from this model class: " + model.getClass)
    }
  }

  def XsYpred[T <: KmeansModel](
    model: T,
    xRDD: RDD[Array[Double]]): RDD[(Array[Double], Int)] = {

    xRDD.map(point => (point, model.predict(point)))
  }

  def XsYpred[T <: TModel](
    model: T,
    dataframe: RDD[Array[Object]],
    xCols: Array[Int]): RDD[(Array[Double], Int)] = {
    model match {
      case m: KmeansModel => {
        require(m.centroids.get(0).size == xCols.size, "Error: dimensions of centroids and data do not match")
        XsYpred(m, dataframe.map(new ParsePoint(xCols, true)).filter(x => x != null))
      }
      case _ => throw new Exception("Don't know how to predict this model")
    }
  }

  def XsYpred[T <: TModel](
    model: T,
    dataframe: SharkDataFrame,
    xCols: Array[Int]): RDD[(Array[Double], Int)] = {
    model match {
      case m: KmeansModel => {
        require(m.centroids.get(0).size == xCols.size, "Error: dimensions of centroids and data do not match")
        XsYpred(m, dataframe.getDataPointTable(xCols))
      }
      case _ => throw new Exception("Don't know how to predict this model")
    }
  }
}
