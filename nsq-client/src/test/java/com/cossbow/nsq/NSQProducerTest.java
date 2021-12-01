package com.cossbow.nsq;

import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.lookup.DefaultNSQLookup;
import com.cossbow.nsq.lookup.NSQLookup;
import com.cossbow.nsq.util.NSQUtil;
import com.cossbow.nsq.util.ThrowoutConsumer;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class NSQProducerTest extends Nsq {

    private static int concurrency = 1;

    private NSQConfig getSnappyConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getDeflateConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    private NSQConfig getSslConfig() throws SSLException {
        final NSQConfig config = new NSQConfig();
        File serverKeyFile = new File(getClass().getResource("/server.pem").getFile());
        File clientKeyFile = new File(getClass().getResource("/client.key").getFile());
        File clientCertFile = new File(getClass().getResource("/client.pem").getFile());
        SslContext ctx = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL)
                .trustManager(serverKeyFile)
                .keyManager(clientCertFile, clientKeyFile).build();
        config.setSslContext(ctx);
        return config;
    }

    private NSQConfig getSslAndSnappyConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getSslAndDeflateConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }


    private NSQProducer defaultConfig() {
        return withConfig(new NSQConfig());
    }

    private NSQProducer withConfig(NSQConfig config) {
        return new NSQProducer(config, Nsq.getNsqdHost());
    }

    @Test
    public void testProduceOneMsgSnappy() throws NSQException, IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        var queue = new LinkedBlockingQueue<String>();
        var topic = "test1";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var producer = withConfig(getSnappyConfig());

        String msg = randomString();
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            queue.offer(message.getObj());
            message.finished();
        }, getSnappyConfig(), NSQUtil.stringDecoder());
        consumer.start();
        var v = queue.take();
        assertEquals(msg, v);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testProduceOneMsgDeflate() throws NSQException, IOException, InterruptedException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        var topic = "test2";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = withConfig(getDeflateConfig());

        String msg = randomString();
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + (message.getObj()));
            counter.incrementAndGet();
            message.finished();
        }, getDeflateConfig(), NSQUtil.stringDecoder());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testProduceOneMsgSsl() throws InterruptedException, NSQException, IOException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        var topic = "test3";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = withConfig(getSslConfig());

        String msg = randomString();
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslConfig(), NSQUtil.stringDecoder());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testProduceOneMsgSslAndSnappy() throws InterruptedException, NSQException, IOException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        var topic = "test4";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = withConfig(getSslAndSnappyConfig());

        String msg = randomString();
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslAndSnappyConfig(), NSQUtil.stringDecoder());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testProduceOneMsgSslAndDeflate() throws InterruptedException, NSQException, IOException, SSLException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        var topic = "test5";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = withConfig(getSslAndDeflateConfig());

        String msg = randomString();
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslAndDeflateConfig(), NSQUtil.stringDecoder());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testSendAndWait() throws InterruptedException, NSQException, IOException {
        var topic = "test6";
        var queue = new LinkedBlockingQueue<String>();
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var conf = new NSQConfig();
        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            queue.offer(message.getObj());
            message.finished();
        }, conf, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = defaultConfig();

        String msg = randomString();
        producer.produce(topic, msg.getBytes());

        var m = queue.take();
        assertEquals("", msg, m);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testProduceMoreMsg() throws NSQException, IOException, InterruptedException {
        var topic = "test7";
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = defaultConfig();

        for (int i = 0; i < 1000; i++) {
            String msg = randomString();
            producer.produce(topic, msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 1000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 1000);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testParallelProducer() throws InterruptedException, IOException {
        var topic = "test8";
        var queue = new LinkedBlockingQueue<String>();
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            queue.offer(message.getObj());
            message.finished();
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = defaultConfig();

        for (int n = 0; n < 5; n++) {
            new Thread(() -> {
                for (int i = 0; i < 1000; i++) {
                    String msg = randomString();
                    producer.produceAsync(topic, msg.getBytes());
                }
            }).start();
        }
        int i = 0;
        while (i < 5000) {
            var s = queue.take();
            Assert.assertNotNull(s);
            i++;
        }
        assertEquals(5000, i);
        producer.shutdown();
        consumer.unSubscribeAsync().join();

        System.out.println(producer.metrics());
    }

    @Test
    public void testMultiMessage() throws NSQException, InterruptedException, IOException {
        var topic = "test9";
        var queue = new LinkedBlockingQueue<String>();
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            queue.offer(message.getObj());
            message.finished();
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = defaultConfig();

        List<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            messages.add((randomString() + "-" + i).getBytes());
        }
        producer.produceMulti(topic, messages);
        producer.shutdown();

        int i = 0;
        while (i < 50) {
            Assert.assertNotNull(queue.take());
            i++;
        }
        assertEquals(50, i);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testMultiMessageStream() throws NSQException, IOException, InterruptedException {
        var topic = "test10";
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
        final var testLen = 10;

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + message.getObj());
            var c = counter.incrementAndGet();
            message.finished();
            if (c >= testLen) {
                synchronized (counter) {
                    counter.notify();
                }
            }
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = defaultConfig();

        var ss = ThreadLocalRandom.current().ints(testLen)
                .<ThrowoutConsumer<ByteBufOutputStream, IOException>>mapToObj(i -> os -> {
                    var w = new OutputStreamWriter(os);
                    w.append(randomString()).append("-").write(i);
                });
        producer.produceMulti(topic, ss);
        producer.shutdown();

        synchronized (counter) {
            counter.wait();
        }
        assertEquals(counter.get(), testLen);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testBackoff() throws InterruptedException, NSQException, IOException {
        var topic = "test11";
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
            LogManager.getLogger(this).info("Processing message: " + (message.getObj()));
            counter.incrementAndGet();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            message.finished();
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.setExecutor(newBackoffThreadExecutor());
        consumer.start();

        NSQProducer producer = defaultConfig();

        for (int i = 0; i < 20; i++) {
            String msg = randomString();
            producer.produce(topic, msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 20) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 20);
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testScheduledCallback() throws NSQException, IOException, InterruptedException {
        var topic = "test12";
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        Thread.sleep(1000);
        assertEquals(1, counter.get());
        consumer.unSubscribeAsync().join();
    }

    @Test
    public void testPubError() {
        var topic = "test13";
        var data = "Hello".getBytes();

        var consumer = new NSQConsumer<>(lookup, topic, "test", concurrency, (message) -> {
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        NSQProducer producer = new NSQProducer(new NSQConfig(), new ServerAddress("non", 1));
        producer.produceAsync(topic, data);

        var s = System.currentTimeMillis();
        var future = producer.produceAsync(topic, data);
        var t = System.currentTimeMillis();
        Assert.assertEquals(s, t);

        try {
            future.join();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof UnknownHostException);
        }
    }

    @Test
    public void testEphemeralTopic() throws NSQException {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = withConfig(getDeflateConfig());

        String msg = randomString();
        producer.produce("testephem#ephemeral", msg.getBytes());
        producer.shutdown();

        Set<ServerAddress> servers = lookup.lookupAsync("testephem#ephemeral").join();
        assertEquals("Could not find servers for ephemeral topic", 1, servers.size());
    }

    @Test
    public void testLookup() {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var addresses = lookup.lookupAsync("test1").join();
        assertEquals("Could not find servers for topic", 1, addresses.size());
    }

    @Test
    public void testDeferredMessage() {
        final var topic = "test15";
        final int defer = 3;

        var cf = new CompletableFuture<Void>();
        var consumer = new NSQConsumer<>(lookup, topic, "default", 1, msg -> {
            try {
                var now = LocalTime.now();
                var data = msg.getObj();
                var st = LocalTime.parse(data);
                var dt = now.getSecond() - st.getSecond();

                System.out.println(now + " - " + st + " = " + dt);

                if (defer != dt) {
                    cf.completeExceptionally(new IllegalStateException("expect " + defer + "ms, but actual " + dt + "ms"));
                }

                msg.finished();
            } catch (Exception e) {
                msg.requeue();
            } finally {
                if (!cf.isDone()) {
                    cf.complete(null);
                }
            }
        }, CONFIG, NSQUtil.stringDecoder());
        consumer.start();

        var producer = newProducer();

        producer.produceAsync(topic, defer * 1000, LocalTime.now().toString().getBytes());

        cf.join();

    }


    static final NSQLookup lookup = new DefaultNSQLookup();

    static {
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
    }

    static NSQProducer newProducer() {
        var addresses = lookup.lookupNodeAsync().join();
        var producer = new NSQProducer(addresses);

        return producer;
    }


    public static ExecutorService newBackoffThreadExecutor() {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1));
    }

    private String randomString() {
        return "Message_" + new Date().getTime();
    }
}
