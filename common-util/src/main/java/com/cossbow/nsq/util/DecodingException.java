package com.cossbow.nsq.util;

import java.io.IOException;

public class DecodingException extends IOException {
    private static final long serialVersionUID = 6584025067121019634L;

    public DecodingException() {
    }

    public DecodingException(String message) {
        super(message);
    }

    public DecodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecodingException(Throwable cause) {
        super(cause);
    }
}
