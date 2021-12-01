package com.cossbow.nsq;

public class NsqException extends RuntimeException {
    private static final long serialVersionUID = -53897079820260742L;

    public NsqException() {
    }

    public NsqException(String message) {
        super(message);
    }

    public NsqException(String message, Throwable cause) {
        super(message, cause);
    }

    public NsqException(Throwable cause) {
        super(cause);
    }

    public NsqException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
