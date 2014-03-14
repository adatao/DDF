package com.adatao.pa.spark;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import shark.api.JavaSharkContext;

public class SharkQueryUtils {
	
	public static Logger LOG = LoggerFactory.getLogger(SharkQueryUtils.class);
	
	/**
	 * This function always returned a non-empty List<String>
	 *  
	 * @param sc
	 * @param cmd
	 * @param adaExceptionCode: this is a incident-specific AdataoExceptionCode
	 * @return an non-empty List<String>
	 * @throws AdataoException with {@code adaExceptionCode} when {@code SharkContext} returns a {@code shark.api.QueryExecutionException}
	 * @throws AdataoException with {@code ERR_SHARK_RESULT_EMPTY} when the query result is empty or there is one value "NULL"
	 */
	public static List<String> sql2ListString(JavaSharkContext sc, String cmd, AdataoExceptionCode adaExceptionCode) throws AdataoException {
		try {
			List<String> res = sc.sql(cmd);
			if (res.size() == 0 || (res.size() > 0 && res.get(0).equals("NULL"))) {
				/**
				 * this happens when data has NA values or there is not data,
				 * we return null
				 */
				throw new AdataoException(AdataoExceptionCode.ERR_SHARK_RESULT_EMPTY, null);
			} else return res;
		} catch (Exception e) { 
			// I cannot catch shark.api.QueryExecutionException directly
			// most probably because of the problem explained in this
			// http://stackoverflow.com/questions/4317643/java-exceptions-exception-myexception-is-never-thrown-in-body-of-corresponding
			LOG.error("Exception: ", e);
			if(e instanceof shark.api.QueryExecutionException) throw new AdataoException(adaExceptionCode, e);
			else throw e;
		}
	}
	
	/**
	 * This function will return a Double value or @{code null} when get exception with code {@code ERR_SHARK_RESULT_EMPTY}
	 * 
	 * @param sc
	 * @param cmd
	 * @param adaExceptionCode: this is a incident-specific AdataoExceptionCode thrown when {@code SharkContext} returns a {@code shark.api.QueryExecutionException}
	 * @return a Double value or null
	 * @throws AdataoException with {@code adaExceptionCode} when {@code SharkContext} returns a {@code shark.api.QueryExecutionException}, or just throw other exceptions if any 
	 */
	public static Double sql2Double(JavaSharkContext sc, String cmd, AdataoExceptionCode adaExceptionCode) throws AdataoException {
		try {
			List<String> res = sql2ListString(sc, cmd, adaExceptionCode);
			try {
				return Double.parseDouble(res.get(0));
			} catch (Exception e) {
				LOG.info("Exception: ", e);
				return Double.NaN;
			}
		} catch (AdataoException e){
			if (e.getCode() == AdataoExceptionCode.ERR_SHARK_RESULT_EMPTY){
				/**
				 * this happens when data has NA values or there is not data,
				 * we return null
				 */
				LOG.error("Exception: ", e);
				return Double.NaN;
			} else throw e;
		} 
	}
	
	/**
	 * This function will return a Long value or 0 when get exception with code {@code ERR_SHARK_RESULT_EMPTY}
	 * 
	 * @param sc
	 * @param cmd
	 * @param adaExceptionCode: this is a incident-specific AdataoExceptionCode thrown when {@code SharkContext} returns a {@code shark.api.QueryExecutionException}
	 * @return a Long value or 0
	 * @throws AdataoException with {@code adaExceptionCode} when {@code SharkContext} returns a {@code shark.api.QueryExecutionException}, or just throw other exceptions if any 
	 */
	public static Long sql2Long(JavaSharkContext sc, String cmd, AdataoExceptionCode adaExceptionCode) throws AdataoException {
		try {
			List<String> res = sql2ListString(sc, cmd, adaExceptionCode);
			try {
				return Long.parseLong(res.get(0));
			} catch (Exception e){
				LOG.info("Exception: ", e);
				return 0L;
			}
		} catch (AdataoException e){
			if (e.getCode() == AdataoExceptionCode.ERR_SHARK_RESULT_EMPTY){
				/**
				 * this happens when data has NA values or there is not data,
				 * we return null
				 */
				LOG.error("Exception: ", e);
				return 0L;
			} else throw e;
		} 
	}
}