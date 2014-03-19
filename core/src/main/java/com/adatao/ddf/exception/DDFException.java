package com.adatao.ddf.exception;


/**
 */
public class DDFException extends Exception {

  private static final long serialVersionUID = 8871762342909779405L;


  public enum DDFExceptionCode {
    ERR_GENERAL("Unable to execute the command", "This is general error message"), 
    ERR_SQL_QUERY_FAILED("Unable to execute the query.", "This error happens when the shark query failed to run"), 
    ERR_SQL_RESULT_EMPTY("Query result is empty", "This error happens when the shark query for sql command return empty");

    private final String msg;
    private final String desc;


    private DDFExceptionCode(String message, String description) {
      msg = message;
      desc = description;
    }

    public String getMsg() {
      return msg != null ? msg : "";
    }

    public String getDesc() {
      return desc != null ? desc : "";
    }

  }


  public DDFException(String msg) {
    super(msg);
  }

  public DDFException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public DDFException(DDFExceptionCode code) {
    super(code.getMsg());
  }

  public DDFException(DDFExceptionCode code, Throwable cause) {
    super(code.getMsg(), cause);
  }

  public DDFException(Throwable cause) {
    super(cause);
  }
}
