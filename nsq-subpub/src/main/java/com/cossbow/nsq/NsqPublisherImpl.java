package com.cossbow.nsq;

import com.cossbow.nsq.util.Encoder;
import com.cossbow.nsq.util.NSQUtil;
import com.cossbow.nsq.util.ThrowoutConsumer;
import io.netty.buffer.ByteBufOutputStream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
class NsqPublisherImpl implements NsqPublisher {

    private final NSQProducer producer;

    NsqPublisherImpl(@NonNull Collection<ServerAddress> addresses, int poolSize) {
        var config = new NSQConfig();
        if (poolSize > 0) {
            config.setPoolSize(poolSize);
        }
        this.producer = new NSQProducer(config, addresses);

        NSQUtil.SCHEDULER.scheduleWithFixedDelay(() -> {
            var m = producer.metrics();
            log.debug("publisher metrics: {}", m);
        }, 5, 15, TimeUnit.SECONDS);

    }


    //

    @Override
    public CompletableFuture<Void> publish(String topic, int defer, Object value, Encoder encoder) {
        if (null == value) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        return publish(topic, defer, os -> encoder.encode(os, value));

    }

    @Override
    public CompletableFuture<Void> publish(String topic, int defer, byte[] value) {
        if (null == value) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        return publish(topic, defer, os -> os.write(value));
    }

    @Override
    public CompletableFuture<Void> publish(String topic, int defer, ThrowoutConsumer<ByteBufOutputStream, IOException> callback) {
        if (null == callback) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        return producer.produceAsync(topic, defer, callback);
    }


    @Override
    public void disconnect() {
        if (null != producer) {
            producer.shutdown();
        }
    }

}
