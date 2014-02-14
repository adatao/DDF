package com.adatao.ddf.exception;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
public class DDFException extends Exception {
  private static final long serialVersionUID = 1L;

  public DDFException(String msg) {
    super(msg);
  }

  public DDFException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public DDFException(Throwable cause) {
    super(cause);
  }
}
