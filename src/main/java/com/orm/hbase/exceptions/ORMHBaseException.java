package com.orm.hbase.exceptions;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public class ORMHBaseException extends Exception {
    private static final long serialVersionUID = -5595744071699050189L;

    public ORMHBaseException() {
    }

    public ORMHBaseException(String msg) {
        super(msg);
    }

    public ORMHBaseException(String msg, Exception e) {
        super(msg, e);
    }

    public ORMHBaseException(String msg, Throwable t) {
        super(msg, t);
    }

    public ORMHBaseException(Exception e) {
        super(e);
    }
}
