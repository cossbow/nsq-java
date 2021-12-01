package com.cossbow.nsq.frames;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ErrorFrame extends NSQFrame {

    public String getErrorMessage() {
        return readData();
    }

    public String toString() {
        return "ERROR: " + readData();
    }
}
