package com.adatao.pa.spark.execution

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.adatao.pa.spark.DataManager.{ DataFrame, MetaInfo }
import com.adatao.pa.spark.{ SparkThread, DataManager }
import com.adatao.pa.spark.types.{ SuccessResult, ExecutorResult }
import scala.collection.JavaConversions._
import java.util.Map
import com.google.gson.Gson
import com.adatao.ML.types.TJsonSerializable
import scala.annotation.tailrec
import io.ddf.DDF
import com.adatao.spark.ddf.analytics.Utils
import io.ddf.DDF
import io.ddf.content.Schema.Column
import io.ddf.content.Schema.ColumnClass
import java.util.HashMap

/*
 * return intensive comprehensive summary
 * 
 */
class AdvanceSummary(dataContainerID: String) extends AExecutor[AdvanceSummary.AdvanceSummaryResult] {

  protected override def runImpl(context: ExecutionContext): AdvanceSummary.AdvanceSummaryResult = {
    val ddfManager = context.sparkThread.getDDFManager();
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ddfManager.getDDF(ddfId) match {
      case x: DDF ⇒ x
      case _ ⇒ throw new IllegalArgumentException(s"Only accept DDF, ddfID: $ddfId")
    }

    val cols = ddf.getSchema().getColumns()

    val arrColNames = cols.map(c => c.getName()).toArray[String]
    val arrColTypes = cols.map(c => if (c.isNumeric()) "integer" else "character").toArray[String]
    val arrColCategories = cols.map(c => if (c.getColumnClass()== null) "NONE" else if(c.getColumnClass().equals(ColumnClass.FACTOR)) "AdataoFactor" else if (c.getColumnClass().equals(ColumnClass.NUMERIC)) "numeric" else "character").toArray[String]
    val arrColUnits = cols.map(c => "NONE").toArray[String]

    var aSummary: AdvanceSummary.AdvanceSummaryResult = new AdvanceSummary.AdvanceSummaryResult(arrColNames, arrColTypes, arrColCategories, arrColUnits)

    return aSummary
  }
}

object AdvanceSummary extends Serializable {
  //advanceSummary 
  class AdvanceSummaryResult(val colnames: Array[String], val coltypes: Array[String], val colcategories: Array[String], val colunits: Array[String]) {

    //for new return empty
    var business_optimization_targets: Map[String, String] = new HashMap[String, String]()
    var default_time_resolution: Array[String] =  new Array[String] (3)
    

    //on the fly aggregation, always empty except HH data set
    var aggregates_summable: Map[String, Map[String, String]] = new HashMap[String, Map[String, String]]()
    val aggregates_onthefly: Map[String, Map[String, String]] = new HashMap[String, Map[String, String]]()
    val aggregates_final: Map[String, Map[String, String]]= new HashMap[String, Map[String, String]]()
    //hardcode business_optimization_targets
    if (colnames.contains("assembly_revision") && colnames.contains("defect_type")) {
      business_optimization_targets.put("assembly_revision", "None")
      business_optimization_targets.put("defect_type", "facility")
    }
    if (colnames.contains("product")) {
      business_optimization_targets.put("product", "None")
    }
    if (colnames.contains("rating")) {
      business_optimization_targets.put("rating", "ASC")
    }
    if (colnames.contains("arrdelay") && colnames.contains("depdelay")) {
      business_optimization_targets.put("arrdelay", "MIN")
      business_optimization_targets.put("depdelay", "MIN")

      new HashMap[String, String]() {
        {
          put("text", "flight frequency")
          put("sql_expression", "count(*)")
        }
      }

      aggregates_onthefly.put("flight_frequency", new HashMap[String, String]() {
        {
          put("text", "flight frequency")
          put("sql_expression", "count(*)")
        }
      })

      aggregates_onthefly.put("delay_frequency", new HashMap[String, String]() {
        {
          put("text", "delay frequency")
          put("sql_expression", "count(case when arrdelay > 10 then 1 else 0)")
        }
      })

      aggregates_onthefly.put("cancel_frequency", new HashMap[String, String]() {
        {
          put("text", "cancellation frequency")
          put("sql_expression", "sum(cancelled)")
        }
      })

      aggregates_final.put("delay_rate", new HashMap[String, String]() {
        {
          put("text", "delay rate")
          put("depends", "delay_frequency, flight_frequency")
          put("r_expression", "delay_frequency / flight_frequency")
        }
      })

      aggregates_final.put("cancel_rate", new HashMap[String, String]() {
        {
          put("text", "cancellation rate")
          put("depends", "cancel_frequency, flight_frequency")
          put("r_expression", "cancel_frequency / flight_frequency")
        }
      })
    }

    //hardcode default time resolution
    default_time_resolution(0) = "year"
    default_time_resolution(1) = "month"
    default_time_resolution(2) = "day"

    //aggregates_onthefly
    if (colnames.contains("yield")) {
      aggregates_onthefly.put("yield", new HashMap[String, String]() { { put("text", "yield") } })
    }

    //aggregates_summable
    if (colnames.contains("tcp_conn_num")) {
      aggregates_summable.put("TCP_conn_num", new HashMap[String, String]() {
        {
          put("text", "TCP connection number")
          put("column", "tcp_conn_num")
        }
      })

      aggregates_summable.put("TCP_conn_success", new HashMap[String, String]() {
        {
          put("text", "TCP successful connection number")
          put("column", "tcp_conn_success")
        }
      })

      aggregates_summable.put("get_request_num", new HashMap[String, String]() {
        {
          put("text", "GET request number")
          put("column", "get_request_num")
        }
      })

      aggregates_summable.put("get_response_num", new HashMap[String, String]() {
        {
          put("text", "GET response number")
          put("column", "get_response_num")
        }
      })
      aggregates_summable.put("get_response_num", new HashMap[String, String]() {
        {
          put("text", "GET response number")
          put("column", "get_response_num")
        }
      })
      aggregates_summable.put("intbuffer_request_num", new HashMap[String, String]() {
        {
          put("text", "Initial buffer request number")
          put("column", "intbuffer_request_num")
        }
      })
      aggregates_summable.put("intbuffer_success", new HashMap[String, String]() {
        {
          put("text", "Initial successful buffer number")
          put("column", "intbuffer_success")
        }
      })

      aggregates_final.put("TCP_conn_success_rate", new HashMap[String, String]() {
        {
          put("text", "TCP connection success rate")
          put("depends", "TCP_conn_success_rate, TCP_conn_num")
          put("r_expression", "TCP_conn_success_rate / TCP_conn_num")
        }
      })

      aggregates_final.put("TCP_conn_success_rate", new HashMap[String, String]() {
        {
          put("text", "GET success rate")
          put("depends", "GET_response_num, GET_request_num")
          put("r_expression", "GET_response_num / GET_request_num")
        }
      })
      aggregates_final.put("intbuf_success_rate", new HashMap[String, String]() {
        {
          put("text", "initial buffer success rate")
          put("depends", "intbuffer_success, intbuffer_request_num")
          put("r_expression", "intbuffer_success / intbuffer_request_num")
        }
      })
      aggregates_final.put("streaming_response_success_rate", new HashMap[String, String]() {
        {
          put("text", "streaming response success rate")
          put("depends", "intbuffer_success, tcp_conn_num")
          put("r_expression", "intbuffer_success / tcp_conn_num")
        }
      })
    }

  }
}
