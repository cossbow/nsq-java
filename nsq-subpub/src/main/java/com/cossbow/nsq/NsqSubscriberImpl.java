package com.cossbow.nsq;


import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.lookup.NSQLookup;
import com.cossbow.nsq.util.Decoder;
import com.cossbow.nsq.util.LambdaUtil;
import com.cossbow.nsq.util.NSQUtil;
import com.cossbow.nsq.util.ThrowoutFunction;
import io.netty.buffer.ByteBufInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;


class NsqSubscriberImpl implements NsqSubscriber {
    static final Logger log = LoggerFactory.getLogger(NsqSubscriberImpl.class);

    private static final Consumer<NSQException> EXCEPTION_HANDLER = ex -> log.warn("NSQ Error", ex);

    private final NSQLookup nsqLookup;

    private final long lookupPeriodMillis;
    private final int defaultAttemptLimit;
    private final int defaultAttemptDelay;
    private final long defaultBatchInterval;

    private final String userAgent;
    private final boolean autoTouch;

    private final Map<String, List<NSQConsumer<?>>> consumers = new ConcurrentHashMap<>();

    private final ScheduledExecutorService executor;

    NsqSubscriberImpl(
            NSQLookup nsqLookup, long lookupPeriodMillis, int defaultAttemptLimit,
            int defaultAttemptDelay, int schedulerPoolSize, long defaultBatchInterval,
            String userAgent, boolean autoTouch) {
        if (lookupPeriodMillis <= 0) {
            throw new IllegalArgumentException("'lookupPeriodMillis' must be positive");
        }
        if (defaultAttemptLimit < 0) {
            throw new IllegalArgumentException("'defaultAttemptLimit' can not be negative");
        }
        if (defaultAttemptDelay < 0) {
            throw new IllegalArgumentException("'defaultAttemptDelay' can not be negative");
        }
        if (schedulerPoolSize <= 0) {
            throw new IllegalArgumentException("'schedulerPoolSize' must be positive");
        }
        if (defaultBatchInterval <= 0) {
            throw new IllegalArgumentException("'defaultBatchInterval' must be positive");
        }

        this.nsqLookup = nsqLookup;

        this.lookupPeriodMillis = lookupPeriodMillis;
        this.defaultAttemptLimit = defaultAttemptLimit;
        this.defaultAttemptDelay = defaultAttemptDelay;
        this.defaultBatchInterval = defaultBatchInterval;
        this.userAgent = userAgent;
        this.autoTouch = autoTouch;

        this.executor = Executors.newScheduledThreadPool(schedulerPoolSize);
    }

    private NSQConfig newConfig() {
        var config = new NSQConfig();
        config.setUserAgent(userAgent);
        config.setAutoTouch(autoTouch);
        return config;
    }


    private void addConsumer(String topic, String channel, NSQConsumer<?> consumer) {
        consumers.compute(mapKey(topic, channel), (k, li) -> {
            if (null == li) {
                li = new ArrayList<>(1);
            }
            li.add(consumer);
            return li;
        });
    }

    private long attemptsDelay(int attempts) {
        return 1_000 * (4L << attempts);
    }

    private IntToLongFunction getAttemptsDelay(int attemptsLimit) {
        if (attemptsLimit < 0) {
            // not attempt
            return PubSubUtil.attemptNot();
        } else if (attemptsLimit == 0) {
            // attempts always, try once per default
            return PubSubUtil.attemptAlways(defaultAttemptDelay);
        } else {
            // attempts with limit, by delay
            return PubSubUtil.attemptLimit(this::attemptsDelay, attemptsLimit);
        }
    }


    @Override
    public <T> void subscribe(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, decoder, consumer, concurrency, getAttemptsDelay(defaultAttemptLimit));
    }

    public <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, int attemptsLimit) {
        subscribe(topic, channel, type, decoder, consumer, concurrency, getAttemptsDelay(attemptsLimit));
    }

    @Override
    public <T> void subscribe(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("threads must not negative");
        }

        final Consumer<NSQMessage<T>> callback = message -> {
            try {
                var t = message.getObj();
                log.trace("[{}#{}]message[attempts={}, time={}]: {}",
                        topic, channel, message.getAttempts(), message.getTimestamp(), t);
                consumer.accept(t);
                message.finished();
            } catch (Throwable e) {
                dealErrorOrAttempt(message, attemptDelay, topic, channel, e);
            }
        };
        var config = newConfig();
        var c = new NSQConsumer<>(nsqLookup, topic, channel, concurrency, callback, config, decoder, EXCEPTION_HANDLER);
        c.setLookupPeriod(lookupPeriodMillis);
        c.start();

        addConsumer(topic, channel, c);

    }


    //
    //
    //


    //
    //
    //

    @Override
    public <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency) {
        subscribeAsync(topic, channel, type, process, concurrency, defaultAttemptLimit);
    }

    @Override
    public <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, int attemptsLimit) {
        subscribeAsync(topic, channel, type, process, concurrency, getAttemptsDelay(attemptsLimit));
    }


    @Override
    public <T> void subscribeAsync(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder, Function<T, CompletableFuture<?>> process, int concurrency, IntToLongFunction attemptDelay) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("threads must not negative");
        }

        final Consumer<NSQMessage<T>> callback = message -> {
            try {
                var t = message.getObj();
                log.trace("[{}#{}]message[attempts={}, time={}]: {}",
                        topic, channel, message.getAttempts(), message.getTimestamp(), t);
                var future = process.apply(t);
                if (null == future) {
                    message.finished();
                    return;
                }
                future.whenComplete((v, ex) -> {
                    if (null == ex) {
                        // success
                        message.finished();
                    } else {
                        if (ex instanceof CompletionException) ex = ex.getCause();
                        dealErrorOrAttempt(message, attemptDelay, topic, channel, ex);
                    }
                });

            } catch (Throwable e) {
                dealErrorOrAttempt(message, attemptDelay, topic, channel, e);
            }
        };
        var config = newConfig();
        var c = new NSQConsumer<>(nsqLookup, topic, channel, concurrency, callback, config, decoder, EXCEPTION_HANDLER);
        c.setLookupPeriod(lookupPeriodMillis);
        c.start();

        addConsumer(topic, channel, c);
    }

    private void dealErrorOrAttempt(NSQMessage<?> message, IntToLongFunction attemptDelay,
                                    String topic, String channel, Throwable ex) {
        long delay = attemptDelay.applyAsLong(message.getAttempts());
        boolean onlyRetry = ex instanceof RetryDeferEx;
        if (onlyRetry) {
            delay = ((RetryDeferEx) ex).getDefer(attemptDelay.applyAsLong(message.getAttempts()));
        } else if (!(ex instanceof RuntimeException)) {
            message.finished();
            log.error("[{}#{}]message error, no attempt", topic, channel, ex);
            return;
        }
        // exception occur, retry

        if (delay > 0) {
            message.requeue((int) delay);
            if (onlyRetry) {
                log.debug("[{}#{}]message need retry: attempt {} times, try {}ms latter",
                        topic, channel, message.getAttempts(), delay);
            } else {
                log.warn("[{}#{}]message error, attempt {} times, try {}ms latter: {}",
                        topic, channel, message.getAttempts(), delay, ex.getMessage());
            }
        } else if (delay == 0) {
            message.requeue();
            if (onlyRetry) {
                log.debug("[{}#{}]message need retry: attempt {} times, try immediately",
                        topic, channel, message.getAttempts());
            } else {
                log.warn("[{}#{}]message error, attempt {} times, try immediately: {}",
                        topic, channel, message.getAttempts(), ex.getMessage());
            }
        } else {
            message.finished();
            if (onlyRetry) {
                log.info("[{}#{}]message need retry: attempt out times", topic, channel);
            } else {
                log.error("[{}#{}]message error: attempt out times, {}",
                        topic, channel, ex.getMessage());
            }
        }
    }


    public <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Decoder decoder,
                                        Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process, int concurrency) {
        subscribeBatchAsync(topic, channel, type, decoder, filter, process, concurrency,
                getAttemptsDelay(defaultAttemptLimit), defaultBatchInterval);
    }

    public <T> void subscribeBatchAsync(String topic, String channel, Class<T> type, Decoder decoder,
                                        Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                        int concurrency, int attemptsLimit) {
        subscribeBatchAsync(topic, channel, type, decoder, filter, process, concurrency,
                getAttemptsDelay(attemptsLimit), defaultBatchInterval);
    }


    @Override
    public <T> void subscribeBatchAsync(String topic, String channel, ThrowoutFunction<ByteBufInputStream, T, IOException> decoder,
                                        Predicate<T> filter, Function<List<T>, CompletableFuture<List<T>>> process,
                                        int concurrency, IntToLongFunction attemptDelay, long batchInterval) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("threads must be positive");
        }
        if (batchInterval <= 0) {
            throw new IllegalArgumentException("batchInterval must be positive");
        }


        final var queue = new ConcurrentLinkedQueue<NSQMessage<T>>();

        var config = newConfig();
        final Consumer<NSQMessage<T>> callback = message -> {
            T t = message.getObj();
            try {
                if (null != filter && !filter.test(t)) {
                    message.finished();
                    return;
                }
                log.trace("[{}#{}]message[attempts={}, time={}]: {}",
                        topic, channel, message.getAttempts(), message.getTimestamp(), t);

                queue.offer(message);

            } catch (Throwable e) {
                log.error("[{}#{}]batch error", topic, channel, e);
                message.requeue(defaultAttemptDelay);
            }
        };
        var c = new NSQConsumer<>(nsqLookup, topic, channel,
                concurrency, callback, config, decoder, EXCEPTION_HANDLER);
        c.setLookupPeriod(lookupPeriodMillis);

        final Runnable batchTask = () -> executeBatch(topic, channel,
                queue, process, concurrency, attemptDelay, c.getExecutor());
        executor.scheduleAtFixedRate(batchTask, 10, batchInterval, TimeUnit.MILLISECONDS);

        c.start();


        addConsumer(topic, channel, c);

    }


    private final RetryDeferEx defaultRetryDeferEx = new RetryDeferEx();

    private <T> void executeBatch(final String topic, final String channel,
                                  final Queue<NSQMessage<T>> queue,
                                  final Function<List<T>, CompletableFuture<List<T>>> process,
                                  final int concurrency, final IntToLongFunction attemptDelay,
                                  final Executor executor) {
        if (queue.isEmpty()) {
            return;
        }

        var msgList = new ArrayList<NSQMessage<T>>(concurrency);
        // 最多获取concurrency
        for (int i = 0; i < concurrency; i++) {
            var w = queue.poll();
            if (null == w) {
                break;
            }
            msgList.add(w);
        }
        log.debug("[{}#{}]catch {} messages", topic, channel, msgList.size());


        executor.execute(() -> {
            CompletableFuture<List<T>> future;
            try {
                var list = new ArrayList<T>();
                for (var m : msgList) list.add(m.getObj());
                future = process.apply(list);
                if (null == future) {
                    for (var m : msgList) m.finished();
                    return;
                }
            } catch (Throwable e) {
                dealErrorOrAttemptBatch(msgList, attemptDelay, topic, channel, e);
                return;
            }

            future.whenComplete((retryList, ex) -> {
                if (null == ex) {
                    // success
                    if (null == retryList || retryList.isEmpty()) {
                        for (var m : msgList) m.finished();
                        return;
                    }
                    // retry parts of list
                    for (var m : msgList) {
                        if (retryList.contains(m.getObj())) {
                            dealErrorOrAttempt(m, attemptDelay, topic, channel, defaultRetryDeferEx);
                        } else {
                            m.finished();
                        }
                    }
                } else {
                    if (ex instanceof CompletionException) ex = ex.getCause();
                    // exception occur, retry
                    dealErrorOrAttemptBatch(msgList, attemptDelay, topic, channel, ex);
                }
            });
        });
    }


    private <T> void dealErrorOrAttemptBatch(
            List<NSQMessage<T>> messages, IntToLongFunction attemptDelay,
            String topic, String channel, Throwable e) {
        for (var m : messages) {
            dealErrorOrAttempt(m, attemptDelay, topic, channel, e);
        }
    }


    //
    //
    //


    @Override
    public CompletableFuture<Void> unSubscribe(String topic, String channel) {
        var mk = mapKey(topic, channel);
        log.info("unSubscribe: {}/{}", topic, channel);
        return NSQUtil.unSubscribe(consumers.remove(mk));
    }

    @Override
    public CompletableFuture<Void> unSubscribeAll() {
        log.info("unSubscribe all");
        synchronized (consumers) {
            var cs = new HashMap<>(consumers);
            consumers.clear();
            var list = cs.values().stream()
                    .flatMap(LambdaUtil.collectionStream())
                    .collect(Collectors.toList());
            return NSQUtil.unSubscribe(list);
        }
    }

    public Set<String> topics() {
        return new HashSet<>(consumers.keySet());
    }

    private static String mapKey(String topic, String channel) {
        return topic + ':' + channel;
    }

}
