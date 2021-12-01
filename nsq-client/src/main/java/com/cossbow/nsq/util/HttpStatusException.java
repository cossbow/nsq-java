package com.cossbow.nsq.util;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * http请求状态异常
 */
public class HttpStatusException extends RuntimeException {
    private static final long serialVersionUID = 5683334801585213943L;

    private HttpResponseStatus httpStatus;

    public HttpStatusException() {
    }

    public HttpStatusException(HttpResponseStatus httpStatus) {
        super(httpStatus.toString());
        this.httpStatus = httpStatus;
    }

    public HttpStatusException(String message) {
        super(message);
    }

    public HttpStatusException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpStatusException(Throwable cause) {
        super(cause);
    }

    public HttpStatusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public int getHttpStatus() {
        return httpStatus.code();
    }

}
