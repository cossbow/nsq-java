package com.cossbow.nsq.util;

@FunctionalInterface
public interface ThrowoutConsumer<T, E extends Throwable> {

    void accept(T t) throws E;

}
