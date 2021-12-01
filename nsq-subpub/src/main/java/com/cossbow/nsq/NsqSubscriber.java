package com.cossbow.nsq;




import com.cossbow.nsq.util.Decoder;
import com.cossbow.nsq.util.Serializer;
import com.cossbow.nsq.util.ThrowoutFunction;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;

/**
 * nsq订阅消息
 * ** 考虑改装成builder模式，未完成
 */
public interface NsqSubscriber {

    /**
     * 默认解码器
     */
    Decoder defaultDecoder = PubSubUtil.getDefaultDecoder();

    static <T> ThrowoutFunction<ByteBufInputStream, T, IOException>
    newDecoder(Decoder decoder, Class<T> type) {
        return is -> decoder.decode(is, type);
    }

    //
    // 同步消费方法
    //

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer) {
        subscribe(topic, channel, type, defaultDecoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer) {
        subscribe(topic, channel, type, decoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Consumer<T> consumer) {
        subscribe(topic, channel, decoder, consumer, 1);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, newDecoder(decoder, type), consumer, concurrency);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    <T> void subscribe(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency);

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency, int attemptsLimit) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency, attemptsLimit);
    }

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, int attemptsLimit);

    /**
     * @see #subscribe(String, String, Class, Decoder, Consumer, int, IntToLongFunction)
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        subscribe(topic, channel, type, defaultDecoder, consumer, concurrency, attemptDelay);
    }

    /**
     * 同步消费
     *
     * @param topic        话题名称
     * @param channel      消息订阅通道
     * @param type         消息类型
     * @param decoder      消息解码器，{@link Decoder}{@link Serializer}
     * @param consumer     消费回调
     * @param concurrency  并发数
     * @param attemptDelay 获取尝试延迟的回调。返回>0则延迟尝试，单位毫秒；0立即尝试，小于0则结束不再尝试
     */
    default <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        subscribe(topic, channel, newDecoder(decoder, type), consumer, concurrency, attemptDelay);
    }

    <T> void subscribe(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay);


    //
    // 异步消费方法
    //

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process) {
        subscribeAsync(topic, channel, type, process, 1);
    }

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency);

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, int attemptsLimit);

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, type, process, 1, attemptDelay);
    }

    /**
     * @see #subscribeAsync(String, String, Class, Decoder, Function, int, IntToLongFunction)
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, type, defaultDecoder, process, concurrency, attemptDelay);
    }

    /**
     * 异步消费
     *
     * @param topic        话题名称
     * @param channel      消息订阅通道
     * @param type         消息类型
     * @param process      消费函数，返回一个{@link CompletableFuture}
     * @param attemptDelay 获取尝试延迟的回调。返回>0则延迟尝试，单位毫秒；0立即尝试，小于0则结束不再尝试
     */
    default <T> void subscribeAsync(String topic, String channel, Class<T> type, Decoder decoder, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay) {
        subscribeAsync(topic, channel, newDecoder(decoder, type), process, concurrency, attemptDelay);
    }

    <T> void subscribeAsync(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay);


    //
    //
    //


    /**
     * @see #subscribeBatchAsync(String, String, Class, Decoder, Predicate, Function, int, IntToLongFunction, long)
     */
    default <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process, int concurrency) {
        subscribeBatchAsync(topic, channel, type, defaultDecoder, filter, process, concurrency);
    }

    /**
     * @see #subscribeBatchAsync(String, String, Class, Decoder, Predicate, Function, int, IntToLongFunction, long)
     */
    <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Decoder decoder, Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process, int concurrency);

    /**
     * @see #subscribeBatchAsync(String, String, Class, Decoder, Predicate, Function, int, IntToLongFunction, long)
     */
    default <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                         int concurrency, int attemptsLimit) {
        subscribeBatchAsync(topic, channel, type, defaultDecoder, filter, process, concurrency, attemptsLimit);
    }

    /**
     * @see #subscribeBatchAsync(String, String, Class, Decoder, Predicate, Function, int, IntToLongFunction, long)
     */
    <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Decoder decoder, Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                 int concurrency, int attemptsLimit);

    /**
     * @see #subscribeBatchAsync(String, String, Class, Decoder, Predicate, Function, int, IntToLongFunction, long)
     */
    default <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                         int concurrency, IntToLongFunction attemptDelay, long batchInterval) {
        subscribeBatchAsync(topic, channel, type, defaultDecoder, filter, process, concurrency, attemptDelay, batchInterval);
    }

    /**
     * 异步批量消费
     *
     * @param topic         话题名称
     * @param channel       消息订阅通道
     * @param type          消息类型
     * @param filter        消息过滤器，null则不过滤
     * @param process       消费函数，返回一个{@link CompletableFuture<List>} 参数是消息集合，空或null则全部成功，返回的列表需要重试（部分重试）；异常则全部重试
     * @param attemptDelay  获取尝试延迟的回调。返回>0则延迟尝试，单位毫秒；0立即尝试，小于0则结束不再尝试
     * @param batchInterval 批量轮询的周期，毫秒
     */
    default <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Decoder decoder,
                                         Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                         int concurrency, IntToLongFunction attemptDelay, long batchInterval) {
        subscribeBatchAsync(topic, channel, newDecoder(decoder, type), filter, process, concurrency, attemptDelay, batchInterval);
    }

    <T> void subscribeBatchAsync(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder,
                                 Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                 int concurrency, IntToLongFunction attemptDelay, long batchInterval);


    //
    //
    //

    /**
     * 退订
     *
     * @param topic   话题名称
     * @param channel 消息订阅通道
     */
    CompletableFuture<Void> unSubscribe(String topic, String channel);

    /**
     * 退订所有订阅
     */
    CompletableFuture<Void> unSubscribeAll();

}
