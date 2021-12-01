package com.cossbow.nsq.util;

@FunctionalInterface
public interface ThrowoutFunction<T, R, E extends Throwable> {

    R apply(T t) throws E;

}
