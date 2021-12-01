package com.cossbow.nsq;


import com.cossbow.nsq.util.*;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.util.function.IntToLongFunction;

final
public class PubSubUtil {
    private PubSubUtil() {
    }

    private final static Serializer defaultSerializer = JdkSerializer.INSTANCE;

    public static Encoder getDefaultEncoder() {
        return defaultSerializer;
    }

    public static Decoder getDefaultDecoder() {
        return defaultSerializer;
    }


    public static ThrowoutConsumer<ByteBufOutputStream, IOException> stringEncoder(String value) {
        return NSQUtil.stringEncoder(value);
    }

    public static ThrowoutFunction<ByteBufInputStream, String, IOException> stringDecoder() {
        return NSQUtil.stringDecoder();
    }


    //
    //
    //

    final static int ATTEMPT_STOP_VALUE = -1;

    public static IntToLongFunction attemptNot() {
        return a -> ATTEMPT_STOP_VALUE;
    }

    public static IntToLongFunction attemptAlways(int delay) {
        return a -> delay;
    }

    public static IntToLongFunction attemptLimit(int delay, int limit) {
        return attemptLimit(a -> delay, limit);
    }

    public static IntToLongFunction attemptLimit(IntToLongFunction get, int limit) {
        return a -> {
            if (a < limit) {
                return get.applyAsLong(a);
            } else {
                return ATTEMPT_STOP_VALUE;
            }
        };
    }

}
