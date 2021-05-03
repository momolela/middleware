package com.momolela.cachecentre.exception;

public class RedisInitializerException extends ExceptionInInitializerError {
    private static final long serialVersionUID = -5429732650470666543L;

    public RedisInitializerException(String msg) {
        super(msg);
    }

    public RedisInitializerException(Throwable cause) {
        super(cause);
    }
}
