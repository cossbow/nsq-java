package com.cossbow.nsq.util;

public interface ThrowoutSupplier<T, E extends Throwable> {

    /**
     * Gets a result.
     *
     * @return a result
     * @throws E throwout exception or error
     */
    T get() throws E;

}
