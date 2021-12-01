package com.cossbow.nsq.exceptions;

public class NSQDException extends RuntimeException {
    private static final long serialVersionUID = 3017022478001141062L;

    public NSQDException() {
    }

    public NSQDException(String message) {
        super(message);
    }

    public NSQDException(String message, Throwable cause) {
        super(message, cause);
    }

    public NSQDException(Throwable cause) {
        super(cause);
    }

    public NSQDException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
