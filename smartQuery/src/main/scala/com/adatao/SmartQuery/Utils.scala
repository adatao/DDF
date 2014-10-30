package com.adatao.SmartQuery
import java.util.{List => JList}
import breeze.linalg._
import breeze.plot._
import scala.collection.JavaConversions._
/**
 * author: daoduchuan
 */
object Utils {

  def plotData(data: JList[Array[Object]]) = {
//    val yTrue = data.map{
//      row => row(0).asInstanceOf[Double]
//    }.toArray
//    val yPred = data.map{
//      row => row(1).asInstanceOf[Double]
//    }.toArray
//    val length = data.size()
//    val x = (0 until data.size()).map{item => item.toDouble}
    val f = Figure()
    //val p = f.subplot(0)
//    val residuals = data.map {
//      row => val result = (row(0).asInstanceOf[Double] - row(1).asInstanceOf[Double])
//        result
//    }.toArray
    val p2 = f.subplot(2,1,1)
    //val g = breeze.stats.distributions.Gaussian(0,1)
    //p2 += hist(g.sample(100000),100)
    //p += plot(x, residuals)
  }
}
