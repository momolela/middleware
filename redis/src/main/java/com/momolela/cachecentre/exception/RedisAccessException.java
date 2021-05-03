package com.momolela.cachecentre.exception;

public class RedisAccessException extends Exception {

    private static final long serialVersionUID = 6056139775315160208L;

    public RedisAccessException(String msg) {
        super(msg);
    }

    public RedisAccessException(Throwable cause) {
        super(cause);
    }

    public RedisAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
