package com.adatao.spark.ddf.etl

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.rosuda.REngine.REXP
import org.rosuda.REngine.REXPDouble
import org.rosuda.REngine.REXPInteger
import org.rosuda.REngine.REXPString
import org.rosuda.REngine.Rserve.RConnection

import com.adatao.spark.ddf.SparkDDF
import com.adatao.ddf.DDF
import com.adatao.ddf.content.Schema
import com.adatao.ddf.content.Schema.Column
import com.adatao.ddf.etl.{TransformationHandler => CoreTransformationHandler}
import com.adatao.ddf.exception.DDFException

class TransformationHandler(mDDF: DDF) extends CoreTransformationHandler(mDDF) {

  override def transformNativeRserve(transformExpression: String): DDF = {
    
    val rh = mDDF.getRepresentationHandler
    val dfrdd = rh.get(classOf[RDD[_]], classOf[REXP]).asInstanceOf[RDD[REXP]]

    // process each DF partition in R
    val rMapped = dfrdd.map { partdf ⇒
      try {
        // one connection for each compute job
        val rconn = new RConnection()

        // send the df.partition to R process environment
        val dfvarname = "df.partition"
        rconn.assign(dfvarname, partdf)

        val expr = String.format("%s <- transform(%s, %s)", dfvarname, dfvarname, transformExpression)
        mLog.info("eval expr = {}", expr)
        
        println(">>>>>>>>>>>>.expr=" + expr.toString())

        // compute!
        tryEval(rconn, expr, errMsgHeader = "failed to eval transform expression")

        // transfer data to JVM
        val partdfres = rconn.eval(dfvarname)

        // uncomment this to print whole content of the df.partition for debug
        // rconn.voidEval(String.format("print(%s)", dfvarname))
        rconn.close()

        partdfres
      }
      catch {
        case e: DDFException ⇒ {
          mLog.error("Exception: ", e)
          throw new DDFException("Unable to perform NativeRserve transformation", e)

        }
      }
    }

    // convert R-processed data partitions back to RDD[Array[Object]]
    val (rdd, columnArr) = RDataFrameToArrayObject(rMapped)

    // persist because this RDD is expensive to recompute
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    //        val jsc = context.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
    // add the rdd to data manager
    //        val bigdf = SharkUtils.createSharkDataFrame(new DataFrame(meta, rdd), jsc)
    //        val uid = dm.add(bigdf)

    val newSchema = new Schema(mDDF.getSchemaHandler.newTableName().replace("-", "_"), columnArr.toList);
    
    //SparkDDF(DDFManager manager, RDD<T> rdd, Class<T> unitType, String namespace, String name, Schema schema)
    new SparkDDF(mDDF.getManager, rdd, classOf[Array[Object]], null, null, newSchema)
    //mDDF.getSchemaHandler.getSchema().setColumns(columnArr.toList)
    //mDDF.getRepresentationHandler.set(rdd, classOf[RDD[_]], classOf[Array[_]], classOf[Object])
  }
  /**
   * Eval the expr in rconn, if succeeds return null (like rconn.voidEval),
   * if fails raise AdataoException with captured R error message.
   * See: http://rforge.net/Rserve/faq.html#errors
   */
  def tryEval(rconn: RConnection, expr: String, errMsgHeader: String) {
    rconn.assign(".tmp.", expr)
    val r = rconn.eval("r <- try(eval(parse(text=.tmp.)), silent=TRUE); if (inherits(r, 'try-error')) r else NULL")
    if (r.inherits("try-error")) throw new DDFException(errMsgHeader + ": " + r.asString())
  }

  /**
   * eval the R expr and return all captured output
   */
  def evalCaptureOutput(rconn: RConnection, expr: String): String = {
    rconn.eval("paste(capture.output(print(" + expr + ")), collapse='\\n')").asString()
  }

  /**
   * Convert a RDD of R data.frames into a RDD of Object[]
   */
  def RDataFrameToArrayObject(rdd: RDD[REXP]): (RDD[Array[Object]], Array[Column]) = {

    // get metaInfo of transformed DF
    // and convert R types to Java
    val firstdf = rdd.first()
    val names = firstdf.getAttribute("names").asStrings()
    // detect the output type
    val columns = new Array[Column](firstdf.length)
    for (j ← 0 until firstdf.length()) {
      val bigrType = firstdf.asList().at(j) match {
        case v: REXPDouble ⇒ "double"
        case v: REXPInteger ⇒ "int"
        case v: REXPString ⇒ "string"
        case _ ⇒ throw new DDFException("Only support atomic vectors of type int|double|string!")
      }
      columns(j) = new Column(names(j), bigrType)
    }

    val rddarrobj = rdd.flatMap { partdf ⇒
      val dflist = partdf.asList()
      val partitionSize = (0 until dflist.size()).map(j ⇒ dflist.at(j).length()).reduce { (x, y) ⇒ math.max(x, y) }

      println("partdf.len = " + partdf.length())
      println("partitionSize = " + partitionSize)

      // big allocation!
      val jdata = Array.ofDim[Object](partitionSize, dflist.size())

      // convert R column-oriented AtomicVector to row-oriented Object[]
      // TODO: would be nice to be able to create BigR DataFrame from columnar vectors
      (0 until dflist.size()).foreach { j ⇒
        val rcolvec = dflist.at(j)
        dflist.at(j) match {
          case v: REXPDouble ⇒ {
            val data = rcolvec.asDoubles() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              if (REXPDouble.isNA(data(i)))
                jdata(i)(j) = null
              else
                jdata(i)(j) = data(i).asInstanceOf[Object]
              i += 1
            }
          }
          case v: REXPInteger ⇒ {
            val data = rcolvec.asIntegers() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              if (REXPInteger.isNA(data(i)))
                jdata(i)(j) = null
              else
                jdata(i)(j) = data(i).asInstanceOf[Object]
              i += 1
            }
          }
          case v: REXPString ⇒ {
            val data = rcolvec.asStrings() // no allocation
            var i = 0 // row idx
            while (i < partitionSize) {
              jdata(i)(j) = data(i)
              i += 1
            }
          }
          // TODO: case REXPLogical
        }
      }

      // (0 until partitionSize).foreach { i => println("data(i) = " + util.Arrays.toString(jdata(i))) }

      jdata
    }

    (rddarrobj, columns)
  }

}