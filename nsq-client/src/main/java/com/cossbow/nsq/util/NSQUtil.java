package com.cossbow.nsq.util;

import com.cossbow.nsq.NSQConsumer;
import com.cossbow.nsq.ServerAddress;
import com.cossbow.nsq.util.HttpClientUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.cossbow.nsq.util.FutureUtil;
import com.cossbow.nsq.util.ThrowoutConsumer;
import com.cossbow.nsq.util.ThrowoutFunction;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.DefaultThreadFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.function.Function;

final
public class NSQUtil {
    private NSQUtil() {
    }


    public static final ScheduledExecutorService SCHEDULER
            = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("nsq-schedule"));

    public final static ExecutorService EXECUTOR
            = Executors.newCachedThreadPool(
            new DefaultThreadFactory("nsq-c"));


    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    public static <T> String toJson(T v) {
        try {
            return mapper.writeValueAsString(v);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("serialize error", e);
        }
    }

    public static <T> T fromJson(String src, Class<T> type) {
        try {
            return mapper.readValue(src, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid json data", e);
        }
    }

    public static <T> T fromJson(URL src, Class<T> type) {
        try {
            return mapper.readValue(src, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid json data", e);
        }
    }

    public static <T> T fromJson(InputStream src, Class<T> type) {
        try (src) {
            return mapper.readValue(src, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid json data", e);
        }
    }


    public static <T> Mono<T> get(String uri, Class<T> type) {
        return HttpClientUtil.get(uri, in -> fromJson(in, type));
    }

    public static Mono<String> post(String uri) {
        return HttpClientUtil.post(uri);
    }

    //

    public static ServerAddress parseAddress(String nsqdAddress) {
        var a = nsqdAddress.split(":");
        if (a.length != 2) {
            throw new IllegalArgumentException("illegal address: " + nsqdAddress);
        }
        return new ServerAddress(a[0], Integer.parseInt(a[1]));
    }

    //

    public static ThrowoutConsumer<ByteBufOutputStream, IOException> stringEncoder(String value) {
        return (os) -> os.buffer().writeCharSequence(value, StandardCharsets.UTF_8);
    }

    public static ThrowoutFunction<ByteBufInputStream, String, IOException> stringDecoder() {
        return in -> {
            var reader = new InputStreamReader(in);
            var sb = new StringBuilder();
            var buf = new char[1024];
            int nRead;
            while ((nRead = reader.read(buf)) != -1) {
                sb.append(buf, 0, nRead);
            }
            return sb.toString();
        };
    }

    //

    public
    static CompletableFuture<Void>
    unSubscribe(Collection<NSQConsumer<?>> list) {
        if (null == list || list.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (1 == list.size()) {
            return list.iterator().next().unSubscribeAsync();
        }

        var futures = new ArrayList<CompletableFuture<Void>>();
        for (var consumer : list) {
            futures.add(consumer.unSubscribeAsync());
        }
        return FutureUtil.allOf(futures);
    }

    //

    public static <T> CompletableFuture<T> future(
            ChannelFuture cf,
            Function<Channel, T> call,
            Executor executor) {
        try {
            var future = new CompletableFuture<T>();
            cf.addListener(f -> {
                if (f.isSuccess()) {
                    var ch = cf.channel();
                    future.completeAsync(() -> call.apply(ch),
                            null == executor ? future.defaultExecutor() : executor);
                } else {
                    future.completeExceptionally(f.cause());
                }
            });
            return future;
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public static <T> CompletableFuture<T> future(
            ChannelFuture cf,
            Function<Channel, T> call) {
        return future(cf, call, null);
    }

    public static CompletableFuture<Channel> future(
            ChannelFuture cf) {
        return future(cf, Function.identity());
    }

}
