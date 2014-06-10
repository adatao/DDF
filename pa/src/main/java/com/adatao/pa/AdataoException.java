package com.adatao.pa;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
    
public class AdataoException extends Exception {
  private static final long serialVersionUID = 1L;
  public static Logger LOG = LoggerFactory.getLogger(AdataoException.class);
    
	public enum AdataoExceptionCode {
		ERR_GENERAL("Unable to execute the command",
            "This is general error message"),
//        ERR_NROW_SQL_EMPTY("Unable to execute this command. Please ensure that you created the dataframe correctly, or contact system operators for assistant.",
//            "This error happens when the sql command in NROW executor return an empty array"),
        ERR_SAMPLE_MAX_SIZE("The sample size is bigger than maximum limit. Please reduce the sample size or increase the limit",
            "This is setting error message"),
        ERR_ROC_NOT_BINARY("The label data is not binary classification.",
            "This error happens when running ROC on not non-binary classification data"),
        ERR_ROC_EMPTY("The predicted data is empty.",
            "This error happens when running ROC on not non-binary classification data"),
        ERR_LOAD_TABLE_FAILED("Failed to create the distributed dataframe.",
            "This error happens when the shark query used to load the table failed"),
        ERR_GETDDF_FAILED("Failed to get distributed dataframe by name.",
            "This error happens when ddf name is not existed or wrong ddfName, dataContainerId mapping"),
        ERR_SHARK_QUERY_FAILED("Unable to execute the query.",
            "This error happens when the shark query failed to run"),
        ERR_SHARK_RESULT_EMPTY("The dataframe may have data being NA or there is no data at all",
            "This error happens when the shark query for sql command return empty"),    
        ERR_RSERVE_SYNTAX("Syntax errors in the R transform/mapreduce expressions.",
            "Syntax error in the user-defined R functions"),
        ERR_RSERVE_EVAL("Runtime evaluation of R code failed.",
            "Rserve runtime code evaluation failure"),
        ERR_DATAFRAME_NONEXISTENT("This dataframe does not exist. Please ensure the dataframe was created properly",
            "The Dataframe does not exist"),
        ERR_DATAFRAME_EMPTY("The dataframe is empty. Please ensure the dataframe was created properly",
                "The Dataframe is empty"),
        ERR_UNSUPPORTED_COLUMN_TYPE("Unsurpported column type. You may want to convert this column to factor", 
        		"Column type is not supported in this operation"),
		ERR_HDFS("Cannot execute the hdfs command", "This is for failed hdfs command"),
		ERR_HADOOPHOME_UNDEF("HADOOP_HOME is undefined", "HADOOP_HOME is undefined"),
		ERR_KRYO_BUFFER_OVFL("Got KryoSerializer buffer overflow. Please increase the buffer using spark.kryoserializer.buffer.mb"),
		ERR_UNSUPPORTED_CONTAINER_TYPE("Unsupported data container type", "Unsupported data container type"),
    ERR_UNSUPPORTED_JOIN_SYNTAX("Unsupported join syntax"),
    ERR_UNSUPPORTED_GROUP_SYNTAX("Unsupported group syntax"),
    ERR_MISSING_DATAFRAME("Missing dataframe indentification"),
    ERR_UNAUTHORIZED_ACCESS("Unauthorized access.", 
        "You don't have access rights to this resource. Please contact system operators for assistant."),
    ERR_LBFGS("Got error with LBFGS optimizer.");
		
    	private final String mMessage;
    	private final String mDescription;
    	
    	private AdataoExceptionCode(String message) {
        mMessage = (message != null ? message : "");
        mDescription = mMessage;
      }
    	
    	private AdataoExceptionCode(String message, String description) {
    		mMessage = message;
    		mDescription = description;
    	}
    	
      public String getMessage() {
        return mMessage != null ? mMessage : "";
      }
      
//      public String getDescription() {
//        return mDescription != null ? mDescription : "";
//      }
    	
    }
    
	public static final AdataoException defaultException = new AdataoException(AdataoExceptionCode.ERR_GENERAL, null);
    private AdataoExceptionCode mCode;
	private String mUserMessage;
	
//    public AdataoException(AdataoExceptionCode code) {
//    	mCode = code;
//    }
    
//    public AdataoException(AdataoExceptionCode code, String userMessage) {
//    	mCode = code;
//    	mUserMessage = userMessage;
//    }
    
    public AdataoException(AdataoExceptionCode code, Throwable cause) {
    	super(cause);
    	mCode = code;
    }
    
    public AdataoException(AdataoExceptionCode code, String userMessage, Throwable cause) {
    	super(cause);
    	mCode = code;
    	mUserMessage = userMessage;
    }
    
    public String getMessage() {
    	if (mUserMessage != null) {
    		return String.format("%s: %s (Diagnostic message: %s)", mCode.name(), mCode.getMessage(), mUserMessage);
    	} else {
    		return String.format("%s: %s", mCode.name(), mCode.getMessage());
		}
	}
	
	public String getCodeName() {
		return mCode.name();
	}
	
	public AdataoExceptionCode getCode() {
		return mCode;
	}
}