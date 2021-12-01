package com.cossbow.nsq;

import com.cossbow.nsq.util.*;
import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.frames.NSQFrame;
import com.cossbow.nsq.lookup.NSQLookup;
import io.netty.buffer.ByteBufInputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class NSQConsumer<T> implements Consumer<com.cossbow.nsq.NSQMessage<T>> {

    private static final com.cossbow.nsq.NSQCommand CLS = com.cossbow.nsq.NSQCommand.startClose();

    //

    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final ThrowoutFunction<ByteBufInputStream, T, IOException> decoder;
    private final Consumer<com.cossbow.nsq.NSQMessage<T>> callback;
    private final Consumer<NSQException> errorCallback;
    private final com.cossbow.nsq.NSQConfig config;
    private final Map<com.cossbow.nsq.ServerAddress, com.cossbow.nsq.Connection<T>> connections = new ConcurrentHashMap<>();

    private final com.cossbow.nsq.NSQCommand subscribeCmd, readyCmd;

    private volatile boolean started = false;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ExecutorService executor = NSQUtil.EXECUTOR;


    public NSQConsumer(NSQLookup lookup, String topic, String channel,
                       int concurrency, Consumer<com.cossbow.nsq.NSQMessage<T>> callback, com.cossbow.nsq.NSQConfig config,
                       ThrowoutFunction<ByteBufInputStream, T, IOException> decoder) {
        this(lookup, topic, channel, concurrency, callback, config, decoder, null);
    }


    public NSQConsumer(NSQLookup lookup, String topic, String channel,
                       int concurrency, Consumer<com.cossbow.nsq.NSQMessage<T>> callback, com.cossbow.nsq.NSQConfig config,
                       ThrowoutFunction<ByteBufInputStream, T, IOException> decoder,
                       Consumer<NSQException> errCallback) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errCallback;
        this.decoder = Objects.requireNonNull(decoder);
        //
        var maxFlight = config.getMaxInFlight();
        int messagesPerBatch = null != maxFlight && concurrency > maxFlight ?
                maxFlight : concurrency;
        this.subscribeCmd = com.cossbow.nsq.NSQCommand.subscribe(topic, channel);
        this.readyCmd = com.cossbow.nsq.NSQCommand.ready(messagesPerBatch);
    }

    public synchronized NSQConsumer<T> start() {
        if (!started) {
            started = true;
            //connect once otherwise we might have to wait one lookupPeriod
            NSQUtil.SCHEDULER.scheduleWithFixedDelay(this::lookupAndConnect,
                    0, lookupPeriod, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    private CompletableFuture<com.cossbow.nsq.Connection<T>> connect(com.cossbow.nsq.ServerAddress address) {
        log.debug("[[{}]consumer[{}#{}] connect to {}", topic, channel, hashCode(), address);
        return com.cossbow.nsq.Connection.connect(address, config, this, errorCallback)
                .thenCompose(conn -> conn.send(subscribeCmd)
                        .thenCompose(v -> conn.send(readyCmd))
                        .thenApply(LambdaUtil.just(conn)));
    }

    @Override
    public void accept(final com.cossbow.nsq.NSQMessage<T> message) {
        if (callback == null) {
            message.release();
            log.warn("[{}#{}]NO Callback, dropping message: {}",
                    topic, channel, message);
            return;
        }
        try (var in = message.newMessageStream()) {
            var t = decoder.apply(in);
            message.setObj(t);
        } catch (Throwable e) {
            log.error("[{}#{}]decode message body", topic, channel, e);
            message.finished();
        } finally {
            message.release();
        }
        try {
            executor.execute(() -> callback.accept(message));
        } catch (RejectedExecutionException re) {
            log.trace("[{}#{}]Backing off", topic, channel);
            message.requeue();
        }

    }


    public synchronized void setLookupPeriod(final long periodMillis) {
        if (!started) {
            this.lookupPeriod = periodMillis;
        }
    }

    private void connectRefresh(Set<com.cossbow.nsq.ServerAddress> newAddresses) {
        synchronized (connections) {
            for (final var it = connections.entrySet().iterator(); it.hasNext(); ) {
                var cnn = it.next().getValue();
                if (!cnn.isHealthy()) {
                    //force close
                    cnn.close();
                    it.remove();
                }
            }
            final var oldAddresses = connections.keySet();

            if (newAddresses.isEmpty()) {
                // in case the lookup server is not reachable for a short time we don't we dont want to
                // force close connection
                // just log a message and keep moving
                log.debug("[{}#{}]No NSQLookup server connections or topic does not exist. Try latter...",
                        topic, channel);
                return;
            }
            if (oldAddresses.equals(newAddresses)) {
                log.debug("[{}#{}]No new server found, skip", topic, channel);
                return;
            }
            log.debug("[{}#{}]Addresses NSQ connected to: {}", topic, channel, newAddresses);

            var diff = new HashSet<com.cossbow.nsq.ServerAddress>(4);
            // old - new: close
            diff.addAll(oldAddresses);
            diff.removeAll(newAddresses);
            if (!diff.isEmpty()) {
                log.debug("[{}#{}]close offline NSQd connection: {}", topic, channel, diff);
                for (final com.cossbow.nsq.ServerAddress addr : diff) {
                    log.info("[{}#{}]Remove connection {}", topic, channel, addr);
                    var c = connections.remove(addr);
                    if (null != c) c.closeAsync();
                }
            }

            // new - old: connect
            diff.clear();
            diff.addAll(newAddresses);
            diff.removeAll(oldAddresses);
            if (diff.isEmpty()) return;
            log.debug("[{}#{}]connect new online NSQd connection: {}", topic, channel, diff);
            for (var server : diff) {
                connections.put(server, connect(server).join());
            }
        }
    }

    private CompletableFuture<Void> lookupAndConnect() {
        return lookup.lookupAsync(topic).thenAcceptAsync(this::connectRefresh, executor).exceptionally(e -> {
            if (e instanceof CompletionException) e = e.getCause();
            if (e instanceof HttpStatusException) {
                log.debug("lookup [{}/{}] not exists", topic, channel);
            } else {
                log.warn("lookup [{}/{}] fail", topic, channel, e);
            }
            return null;
        });
    }


    private CompletableFuture<NSQFrame> doUnSubscribeAsync(com.cossbow.nsq.Connection<T> c) {
        var f = c.sendAndRecv(CLS);
        var addr = c.getServerAddress();
        f.whenCompleteAsync((frame, ex) -> {
            if (null == ex) {
                log.info("close subscribe {}: {}", addr, frame.readData());
            } else {
                log.warn("close subscribe {} fail", addr, ex);
            }
        }, executor);
        return f;
    }

    public CompletableFuture<Void> unSubscribeAsync() {
        var list = connections.values()
                .stream().map(this::doUnSubscribeAsync)
                .collect(Collectors.toList());
        return FutureUtil.allOf(list);
    }

    public void unSubscribe() {
        unSubscribeAsync().join();
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadPool.
     */
    public synchronized NSQConsumer<T> setExecutor(final ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    public synchronized Executor getExecutor() {
        return Objects.requireNonNullElse(executor, NSQUtil.EXECUTOR);
    }

    private Set<com.cossbow.nsq.ServerAddress> lookupAddresses() {
        return lookup.lookup(topic);
    }
}
