package com.cossbow.nsq.util;

import java.io.IOException;

public class EncodingException extends IOException {
    private static final long serialVersionUID = 4322147921866235142L;

    public EncodingException() {
    }

    public EncodingException(String message) {
        super(message);
    }

    public EncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public EncodingException(Throwable cause) {
        super(cause);
    }
}
