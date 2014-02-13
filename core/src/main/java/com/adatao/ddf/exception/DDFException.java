package com.adatao.ddf.exception;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
public class DDFException extends Exception {
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
