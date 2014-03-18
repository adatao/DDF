package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;

public class ExecutionResultUtils {
	
	public static Logger LOG = LoggerFactory.getLogger(ExecutionResultUtils.class);
	
	/**
	 * This function is used to process the message of SparkException returned from a failed job execution.
	 * Adatao patch https://github.com/adatao/spark/pull/1 allows Spark to send back info about slaves' exceptions to master. 
	 * This info is embedded in the message of SparkException. The message has the following format:
	 * <pre>"Job aborted: ExceptionFailure: ${slave exception}: ${slave exception message}"</pre>
	 *  If the slaves's exception is AdataoException. The message has the following format:
	 * <pre>"Job aborted: ExceptionFailure: adatao.AdataoException: ${AdataoExceptionCode}: ${exception message}"</pre>
	 * 
	 * 
	 * @param sparkException
	 * @return an AdataoException if any otherwise return {@code null} 
	 */
	
	static final String adataoMsgPrefixRegex = "^Job aborted: ExceptionFailure: adatao\\.AdataoException:.+";
	static final String emptyCollectionRegex = "^empty collection$";
	
	public static AdataoException processException(Exception e) {
		String message = e.getMessage().replace("\n"," ");
		String[] msgSplits = message.split(": ");
		String userMessage = null;
		AdataoExceptionCode mCode;
		// I cannot use instanceof here because SparkException is private to Spark
		if (e.getClass().getName() == "org.apache.spark.SparkException" && 
				msgSplits.length >= 5 && message.matches(adataoMsgPrefixRegex)){			
			mCode = AdataoExceptionCode.valueOf(msgSplits[3].trim());
			String[] msgSplits2 = message.split("\\(Diagnostic message: ");
			if (msgSplits2.length == 2){
				userMessage = String.format("%s",msgSplits2[1].replace(")",""));
				return new AdataoException(mCode, userMessage, e);
			} else {
				return new AdataoException(mCode, e.getMessage(), e);
			}
		} else if (e instanceof AdataoException) {
			return (AdataoException) e;
		} else if (e instanceof java.lang.UnsupportedOperationException &&
				message.matches(emptyCollectionRegex)) {
			return new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_EMPTY, e);
		} else if (e instanceof DDFException) {
			return new AdataoException(AdataoExceptionCode.ERR_DDF, e.getMessage(), e);
		}
		return new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
	
	}
}
