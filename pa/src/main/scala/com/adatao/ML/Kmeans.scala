package com.adatao.ML

import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import com.adatao.ddf.types.Matrix
import com.adatao.ddf.types.Vector
import org.jblas.{DoubleMatrix, Geometry}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._
import java.util
import com.adatao.pa.spark.types.SuccessResult
import com.adatao.pa.spark.DataManager
import shark.api.Row
//import com.adatao.ML.Kmeans.DataPoint
/*
object Kmeans{
	type PointType = (Array[Double], Int, Double)

	class  DataPoint(val x: Array[Double], var numMembers: Int, var distance: Double) extends Serializable

	class ParsePoint(xCols: Array[Int], doCheckData: Boolean) extends Function1[Array[Object], Array[Double]] with Serializable {
		val base0_XCols: ArrayBuffer[Int] = new ArrayBuffer[Int]
		xCols.foreach(x => base0_XCols += x)

		override def apply(dataArray: Array[Object]): Array[Double] = {
			//var result: DataPoint = null
			var x = new ArrayBuffer[Double]
			if(dataArray.length > 0){
				try{
					val dim: Int = base0_XCols.length
					if(doCheckData){
						var i = 0
						while(i < dim){
							dataArray(base0_XCols(i)) match {
								case i: java.lang.Integer => x += i.toDouble
								case d: java.lang.Double => x += d
								case _         => return null
							}
							i += 1
						}
					}
					else{
						//don't need to check data
						var i = 0
						while(i < dim){
							dataArray(base0_XCols(i)) match {
								case i: java.lang.Integer => x += i.toDouble
								case d: java.lang.Double => x += d
							}
							i += 1
						}
					}

				} catch {
					case e: Exception =>{

						throw new Exception(e)
					}
				}

			}
			x.toArray
		}
	}
	class SharkParsePoint(xCols: Array[Int], metaInfo: Array[DataManager.MetaInfo]) extends Function1[Row, Array[Double]]
	with Serializable
	{

		val base0_XCols: ArrayBuffer[Int] = new ArrayBuffer[Int]
		xCols.foreach(x => base0_XCols += x)

		override def apply(row: Row): Array[Double] = row match {
			case null =>  null
			case x => {
				val dim = base0_XCols.length
				val result= new ArrayBuffer[Double]
				var i = 0
				while(i < dim){
					val idx = base0_XCols(i)
					metaInfo(idx).getType() match{
						case "int" => row.getInt(idx) match{
							case null => return null
							case x =>    result += x.toDouble
						}
						case "double" => row.getDouble(idx) match{
							case null => return null
							case x => result += x
						}
						case s => throw new Exception("not supporting type" + s)
					}
					i+=1
				}
				result.toArray
			}
		}
	}
	//Using imperative style for performance gain
	def mapClosest(centroids: Array[PointType], point: Array[Double]): (Int, PointType) = {
			var clusterID = 0
			var closest: Double = Double.PositiveInfinity
			var i = 0
			while(i < centroids.length){
				val c: Array[Double] = centroids(i)._1
				val dim = c.length
				var dist: Double = 0
				var j = 0
				while(j < dim){
					dist = dist + math.pow(c(j) - point(j), 2)
					j = j + 1
				}
				if (dist < closest){
					closest = dist
					clusterID = i
				}
				i = i + 1
			}

			new Tuple2(clusterID, (point, 1, closest))
		}

	class filterFunction extends Function1[Array[Double], Boolean] with Serializable {
		override def apply(point: Array[Double]): Boolean =
			point != null
	}

	def pointsReducer(p1: PointType, p2: PointType): PointType = {
		val px: Array[Double] = new Array[Double](p1._1.length)
		var py: Int = 0
		var i = 0
		while(i < p1._1.length){
			px(i) = (p1._1(i) * p1._2 + p2._1(i)*p2._2) / (p1._2 + p2._2)
			i = i + 1
		}
		(px, p1._2 + p2._2, p1._3 + p2._3)
	}
	def train(points: RDD[Array[Double]], numIterations: Int, centroids: Array[PointType]): KmeansModel = {
		val K = centroids.size

		var closestPoints: RDD[(Int, PointType)] = null
		var total_withinss: Array[Double] = new Array[Double](numIterations)
		var iter = 0
		closestPoints = points.map(x => mapClosest(centroids, x))

		while(iter < numIterations){
			val startTime: Long = System.currentTimeMillis()

			val newCentroids: Map[Int, (Array[Double], Int, Double)] =
				closestPoints.reduceByKey{case(x, y) => pointsReducer(x, y)}.collectAsMap()

			//set new centroids
			newCentroids.foreach(x => centroids(x._1) = x._2)

			//calculate total withins
			total_withinss(iter) = 0.0
			centroids.foreach(x => total_withinss(iter) = total_withinss(iter) + x._3 )

			//allocate each vector to closest centroid
			closestPoints = points.map(x => mapClosest(centroids, x))

			System.out.println("Iteration "+ iter + " : " +
				(System.currentTimeMillis() - startTime) + " ms"
			)
			iter = iter + 1
		}
		val pointsPerCluster: Array[Int] = new Array[Int](centroids.length)
		val centroid_coords: util.List[Array[Double]] = new util.ArrayList[Array[Double]](K)

		var i = 0
		var numSamples: Long = 0
		while(i < K){
			pointsPerCluster(i) = centroids(i)._2
			numSamples += pointsPerCluster(i)
			centroid_coords.add(centroids(i)._1)
			i = i + 1
		}
		return new KmeansModel(total_withinss, pointsPerCluster, centroid_coords, numSamples)
	}
}*/
class KmeansModel(val totalWithins: Double,
									val pointsPerCluster: Array[Int],
								  centroids: java.util.List[Array[Double]],
									numSamples: Long)
		extends AUnsupervisedModel[Array[Double], Int](centroids, numSamples) {
	override def predict(point: Array[Double]): Int = {
		var clusterID = 0
		var closest: Double = Double.PositiveInfinity
		var i = 0
		while(i < centroids.size){
			val c: Array[Double] = centroids.get(i)
			val dim = c.length
			var dist: Double = 0
			var j = 0
			while(j < dim){
				dist = dist + math.pow(c(j) - point(j), 2)
				j = j + 1
			}
			if (dist < closest){
				closest = dist
				clusterID = i;
			}
			i = i + 1
		}
		clusterID
	}
}



