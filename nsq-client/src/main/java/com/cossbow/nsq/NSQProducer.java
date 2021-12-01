package com.cossbow.nsq;

import com.cossbow.nsq.util.LambdaUtil;
import com.cossbow.nsq.util.ThrowoutConsumer;
import com.cossbow.nsq.exceptions.BadMessageException;
import com.cossbow.nsq.exceptions.BadTopicException;
import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.frames.ErrorFrame;
import com.cossbow.nsq.frames.NSQFrame;
import io.netty.buffer.ByteBufOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.cossbow.nsq.NSQCommand.multiPublish;
import static com.cossbow.nsq.NSQCommand.publish;

final
public class NSQProducer {
    private static final Logger log = LoggerFactory.getLogger(NSQProducer.class);


    private final List<com.cossbow.nsq.ServerAddress> addresses;
    private final AtomicInteger roundRobinCount = new AtomicInteger(0);

    private final NSQConfig config;

    private final Map<com.cossbow.nsq.ServerAddress, InstrumentedPool<com.cossbow.nsq.Connection<?>>> connPools;

    public NSQProducer(com.cossbow.nsq.ServerAddress address) {
        this(List.of(address));
    }

    public NSQProducer(Collection<com.cossbow.nsq.ServerAddress> addresses) {
        this(new NSQConfig(), addresses);
    }

    public NSQProducer(NSQConfig config, com.cossbow.nsq.ServerAddress address) {
        this(config, List.of(address));
    }

    public NSQProducer(NSQConfig config, Collection<com.cossbow.nsq.ServerAddress> addresses) {
        if (null == addresses || addresses.isEmpty()) {
            throw new IllegalArgumentException("address required");
        }
        this.config = Objects.requireNonNull(config);
        this.addresses = List.copyOf(new HashSet<>(addresses));
        this.connPools = new ConcurrentHashMap<>();
    }

    private com.cossbow.nsq.ServerAddress fetchServer() {
        if (addresses.isEmpty()) throw new IllegalStateException("not set address");

        var i = roundRobinCount.getAndIncrement() % addresses.size();
        return addresses.get(i);
    }

    private InstrumentedPool<com.cossbow.nsq.Connection<?>> createPool(com.cossbow.nsq.ServerAddress addr) {
        Mono<com.cossbow.nsq.Connection<?>> creator = Mono.defer(() -> {
            log.debug("producer connect to {}", addr);
            return Mono.fromFuture(com.cossbow.nsq.Connection.connect(addr, config,
                    LambdaUtil.doNothing(),
                    LambdaUtil.doNothing()));
        });
        return PoolBuilder.from(creator)
                .destroyHandler(c -> Mono.fromFuture(c.closeAsync()))
                .evictionPredicate((c, md) -> !c.isHealthy())
                .sizeBetween(0, config.getPoolSize())
                .buildPool();
    }

    private InstrumentedPool<com.cossbow.nsq.Connection<?>> fetchConnPool() {
        return connPools.computeIfAbsent(fetchServer(), this::createPool);
    }

    public Map<String, Map<String, Integer>> metrics() {
        var re = new HashMap<String, Map<String, Integer>>(connPools.size());
        for (var en : connPools.entrySet()) {
            var addr = en.getKey();
            var m = en.getValue().metrics();
            re.put(addr.toString(), Map.of(
                    "acquired", m.acquiredSize(),
                    "allocated", m.allocatedSize(),
                    "idleSize", m.idleSize(),
                    "maxAllocated", m.getMaxAllocatedSize()
            ));
        }
        return re;
    }

    <R> CompletableFuture<R> withConnection(
            Function<com.cossbow.nsq.Connection<?>, CompletableFuture<R>> callback) {
        var result = new CompletableFuture<R>();
        fetchConnPool().acquire().retry(5).doOnCancel(() -> {
            result.completeExceptionally(new CancellationException());
        }).doOnError(result::completeExceptionally).doOnSuccess(ref -> {
            if (null == ref) {
                result.completeExceptionally(
                        new IllegalStateException("borrow null ref"));
                return;
            }
            try {
                var connection = ref.poolable();
                var re = callback.apply(connection);
                re.whenComplete((t, e) -> {
                    ref.release().subscribe();
                    if (null == e) {
                        result.complete(t);
                    } else {
                        result.completeExceptionally(e);
                    }
                });
            } catch (Throwable e) {
                ref.release().subscribe();
                result.completeExceptionally(e);
            }
        }).subscribe();
        return result;
    }

    public CompletableFuture<NSQFrame> send(com.cossbow.nsq.NSQCommand command) {
        return withConnection(connection ->
                connection.sendAndRecv(command));
    }

    /**
     * produce multiple messages.
     */
    public void produceMulti(String topic, List<byte[]> messages) throws NSQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
        }

        var future = send(multiPublish(topic, messages));
        var frame = future.join();
        checkErrorFrame(frame);
    }

    public void produceMulti(
            String topic,
            Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> callbackStream)
            throws NSQException {
        if (callbackStream == null) return;

        var future = send(multiPublish(topic, callbackStream));
        var frame = future.join();
        checkErrorFrame(frame);
    }

    public void produce(String topic, byte[] message) throws NSQException {
        var future = send(publish(topic, message));
        var frame = future.join();
        checkErrorFrame(frame);
    }

    public CompletableFuture<Void> produceAsync(String topic, int defer, byte[] message) {
        return withConnection(connection -> {
            var command = defer > 0 ? publish(topic, defer, message) : publish(topic, message);
            return connection.send(command);
        });
    }

    public CompletableFuture<Void> produceAsync(String topic, byte[] message) {
        return produceAsync(topic, 0, message);
    }

    public CompletableFuture<Void> produceAsync(String topic, int defer, ThrowoutConsumer<ByteBufOutputStream, IOException> callback) {
        return withConnection(connection -> {
            var command = defer > 0 ? publish(topic, defer, callback) : publish(topic, callback);
            return connection.send(command);
        });
    }

    public CompletableFuture<Void> produceAsync(String topic, ThrowoutConsumer<ByteBufOutputStream, IOException> callback) {
        return produceAsync(topic, 0, callback);
    }


    //


    private void checkErrorFrame(NSQFrame frame) throws BadMessageException, BadTopicException {
        if (frame instanceof ErrorFrame) {
            String err = frame.readData();
            if (err.startsWith("E_BAD_TOPIC")) {
                throw new BadTopicException(err);
            }
            if (err.startsWith("E_BAD_MESSAGE")) {
                throw new BadMessageException(err);
            }
        }
    }

    public void shutdown() {
        for (var pool : connPools.values()) {
            pool.dispose();
        }
    }
}
