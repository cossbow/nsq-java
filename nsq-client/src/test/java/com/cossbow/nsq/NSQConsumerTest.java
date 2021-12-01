package com.cossbow.nsq;

import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.lookup.DefaultNSQLookup;
import com.cossbow.nsq.lookup.NSQLookup;
import com.cossbow.nsq.util.NSQUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class NSQConsumerTest extends Nsq {

    //duration to wait before auto-requeing a message setting in nsqd, defined with -msg-timeout:
    //Set your timeout -msg-timeout="5s" command line when starting nsqd or changed this constant
    //to the default of 60000.
    private static final long NSQ_MSG_TIMEOUT = 5000;

    private final Logger log = LogManager.getLogger(this);


    @Test
    public void testLongRunningConsumer() throws NSQException, IOException, InterruptedException {
        final var counter = new AtomicInteger(0);
        final var topic = "nsq-client-test-1";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", 1, (message) -> {
            var c = counter.incrementAndGet();
            log.info("Processing message: {}, counter={}", message.getObj(), c);

            synchronized (counter) {
                if (c >= 2) {
                    counter.notifyAll();
                    log.info("完啦！");
                } else {
                    message.requeue(1_000);
                    log.info("还没完");
                }
            }
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        var producer = new NSQProducer(Nsq.getNsqdHost());

        String msg = "test-one-message";
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        synchronized (counter) {
            counter.wait();
        }

        assertEquals("数量不对", 2, counter.get());
        consumer.unSubscribeAsync().join();
    }


    @Test
    public void testConcurrencyConsumer() throws InterruptedException {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        final var topic = "nsq-client-test-0";
        final int messageTotal = 41;

        lookup.lookupNodeAsync().thenAcceptAsync(addresses -> {
            var producer = new NSQProducer(addresses);


            for (int i = 0; i < messageTotal; i++) {
                String msg = "message-" + i;
                System.out.println("send: " + msg);
                producer.produceAsync(topic, msg.getBytes());
            }
        }, executor);

        Thread.sleep(1000);

        var consumers = new ArrayList<NSQConsumer<?>>();
        var queue = new LinkedBlockingQueue<String>();
        {
            var consumer = new NSQConsumer<>(lookup, topic, "test-consumer", 2, message -> {
                String m = message.getObj();
                queue.offer(m);
                printWithOrder("get: " + m);
                sleep(1000);
                message.finished();
            }, CONFIG, NSQUtil.stringDecoder(), Throwable::printStackTrace);
            consumer.start();
            consumers.add(consumer);
        }
        {
            var consumer = new NSQConsumer<>(lookup, topic, "test-consumer", 4, message -> {
                String m = message.getObj();
                queue.offer(m);
                printWithOrder("get: " + m);
                sleep(2000);
                message.finished();

            }, CONFIG, NSQUtil.stringDecoder(), Throwable::printStackTrace);
            consumer.start();
            consumers.add(consumer);
        }

        for (int i = 0; i < messageTotal; i++) {
            var m = queue.take();
            Assert.assertNotNull(m);
        }
        NSQUtil.unSubscribe(consumers).join();
    }

    @Test
    public void testMessageTimeout() throws NSQException, IOException, InterruptedException {
        final var queue = new LinkedBlockingQueue<String>();
        final var topic = "nsq-client-test-2";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
        var timeout = Duration.ofSeconds(6);
        var config = new NSQConfig();
        config.setAutoTouch(false);
        config.setMsgTimeout((int) timeout.toMillis());

        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", 1, (message) -> {
            log.info("Processing message: {}", message.getObj());
            queue.offer(message.getObj());
        }, config, NSQUtil.stringDecoder());
        consumer.start();

        var producer = new NSQProducer(Nsq.getNsqdHost());

        String msg = "test-one-message";
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        queue.take();
        var t0 = Instant.now();
        queue.take();
        var t1 = Instant.now();
        var dt = ChronoUnit.SECONDS.between(t0, t1);

        assertEquals("超时时间不对", timeout.toSeconds(), dt);
        consumer.unSubscribeAsync().join();
    }


    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private void printWithOrder(Object v) {
        executor.execute(() -> System.out.println(v));
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
