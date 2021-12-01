package com.cossbow.nsq.frames;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResponseFrame extends NSQFrame {

    public String getMessage() {
        return readData();
    }

    public String toString() {
        return "RESPONSE: " + readData();
    }
}
