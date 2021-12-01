package com.cossbow.nsq;

import com.cossbow.nsq.util.LambdaUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

@Slf4j
public class NsqTest extends NsqBaseTest {

    private static final String TOPIC_1 = "nsq-test-news-1";
    private static final String TOPIC_2 = "nsq-test-news-2";
    private static final String TOPIC_3 = "nsq-test-news-3";
    private static final String TOPIC_4 = "nsq-test-news-4";
    private static final String TOPIC_5 = "nsq-test-news-5";
    private static final String TOPIC_6 = "nsq-test-news-6";
    private static final String TOPIC_7 = "nsq-test-news-7";
    private static final String TOPIC_8 = "nsq-test-news-8";
    private static final String TOPIC_9 = "nsq-test-news-9";
    private static final String TOPIC_10 = "nsq-test-news-10";


    private static final String DEFAULT_CHANNEL = "ch";


    private long limitAttempts(int attempts, int attemptsLimit) {
        if (attempts < attemptsLimit) {
            return 1_000;
        } else {
            return -1;
        }
    }


    @Test
    public void publish() throws InterruptedException {
        Executor executor = Executors.newCachedThreadPool();

        var queue = new ToQueue<News>(100);
        subscriber.subscribe(TOPIC_1, DEFAULT_CHANNEL, News.class, queue, 3);
        waitEmpty(queue);
        executor.execute(() -> {
            for (int i = 1; i <= 100; i++) {
                var news = create(i + 120);
                publisher.publish(TOPIC_1, news);
                System.out.println("发：" + news);
            }
        });


        for (int i = 0; i < 100; i++) {
            var n = queue.poll(2, TimeUnit.SECONDS);
            Assert.assertNotNull(n);
        }
    }

    @Test
    public void pubString() throws InterruptedException {
        var queue = new ToQueue<String>();
        subscriber.subscribe(TOPIC_2, "def", String.class, queue);

        var m = UUID.randomUUID() + "卧槽🐉🐮🐍🐖"
                + ThreadLocalRandom.current().nextLong();
        publisher.publish(TOPIC_2, m).join();

        var s = queue.take();
        Assert.assertEquals("", m, s);

    }

    @Test
    public void attemptLimit() throws InterruptedException {
        publisher.publish(TOPIC_3, "消息通知");

        int attemptsLimit = 4;
        var a = new AtomicInteger(0);
        subscriber.subscribe(TOPIC_3, DEFAULT_CHANNEL, String.class, news -> {
            System.out.println("获取到（一）: " + news + "; times=" + a.incrementAndGet());
            if (a.get() >= attemptsLimit) {
                synchronized (a) {
                    a.notifyAll();
                }
            }
            throw new NullPointerException();
        }, 1, PubSubUtil.attemptLimit(100, attemptsLimit));

        synchronized (a) {
            a.wait();
        }

        Assert.assertEquals(attemptsLimit, a.get());
        System.out.println(MessageFormat.format("limit={0}, value={1}", attemptsLimit, a.get()));
    }

    @Test
    public void attemptCall() throws InterruptedException {
        var lock = new Object();

        var msg = "消息通知" + UUID.randomUUID();
        int attemptsLimit = 4;
        var a = new AtomicInteger(0);
        subscriber.subscribe(TOPIC_4, DEFAULT_CHANNEL, String.class, s -> {
            if (!msg.equals(s)) return;

            var c = a.incrementAndGet();
            System.out.println("获取到（一）: " + s + "; times=" + c);
            synchronized (lock) {
                if (c == attemptsLimit) {
                    lock.notifyAll();
                }
            }
            throw new RetryDeferEx();
        }, 1, attempts -> limitAttempts(attempts, attemptsLimit));

        publisher.publish(TOPIC_4, msg);

        synchronized (lock) {
            lock.wait();
        }

        Assert.assertEquals("尝试次数不正确", attemptsLimit, a.get());
        System.out.println(MessageFormat.format("limit={0}, value={1}", attemptsLimit, a.get()));
    }


    @Test
    public void unsubscribe() throws InterruptedException {
        var queue = new ToQueue<String>();
        final String ch = "ch-1";

        subscriber.subscribe(TOPIC_5, ch, String.class, queue);
        waitEmpty(queue);

        subscriber.unSubscribe(TOPIC_5, ch);
        System.gc();
        Thread.sleep(1000);
        publisher.publish(TOPIC_5, "消息");
        var v = queue.poll(2, TimeUnit.SECONDS);
        Assert.assertNull(v);

        subscriber.subscribe(TOPIC_5, ch, String.class, queue);
        publisher.publish(TOPIC_5, "消息");

        int count = 0;
        while (true) {
            v = queue.poll(2, TimeUnit.SECONDS);
            if (null == v) break;
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void subscribeAsync() throws InterruptedException {
        final var a = new AtomicInteger(0);
        final String ch = "ch-1";

        int attemptsLimit = 4;

        publisher.publish(TOPIC_6, "https://www.qq.com/");
        Thread.sleep(100);

        subscriber.subscribeAsync(TOPIC_6, ch, String.class, s -> {
            System.out.println("GET " + s);
            var f = get(s);
            return f.thenApply(r -> {
                var c = a.incrementAndGet();
                System.out.println("try " + c + " times, re=" + r.length());
                if (c >= attemptsLimit) {
                    synchronized (ch) {
                        ch.notifyAll();
                    }
                    return null;
                } else {
                    System.out.println("retry..." + c);
                    throw new RetryDeferEx();
                }
            });
        }, 1, attempts -> limitAttempts(attempts, attemptsLimit));


        synchronized (ch) {
            ch.wait();
        }
        Thread.sleep(1000);

        Assert.assertEquals("一致的", attemptsLimit, a.get());

    }

    @Test
    public void multiConsumer() throws InterruptedException {
        String ch = "ch-1";

        var queue = new ToQueue<>();
        subscriber.subscribe(TOPIC_7, ch, String.class, s -> {
            System.out.println("recv1: " + s);
            queue.offer(s);
        }, 2);
        subscriber.subscribe(TOPIC_7, ch, String.class, s -> {
            System.out.println("recv2: " + s);
            queue.offer(s);
        }, 3);
        waitEmpty(queue);

        var total = 100;
        for (int i = 0; i < total; i++) {
            publisher.publish(TOPIC_7, "message-" + i);
        }

        for (int i = 0; i < total; i++) {
            queue.poll();
        }

    }

    @Test
    public void subscribeBatchAsync() throws InterruptedException {
        final Object so = new Object();

        var c = new AtomicInteger(0);
        int total = 10;
        int attemptsLimit = 2;

        subscriber.subscribeBatchAsync(TOPIC_8, "test", String.class, s -> !"message-5".equals(s), (li) -> {
            System.out.println("列表：" + li);
            System.out.println("列表长度：" + li.size());
            if (c.incrementAndGet() >= (total * attemptsLimit)) {
                synchronized (so) {
                    so.notifyAll();
                }
                return CompletableFuture.completedFuture(null);
            } else {
                System.out.println("counter = " + c.get());
                return CompletableFuture.failedFuture(new RetryDeferEx());
            }
        }, 2, PubSubUtil.attemptAlways(1000), 100);

        for (int i = 0; i < total; i++) {
            publisher.publish(TOPIC_8, "message-" + i);
        }

        synchronized (so) {
            so.wait();
        }

        Assert.assertEquals("", total * attemptsLimit, c.get());
    }

    @Test
    public void subscribeBatchAsyncRetry() throws InterruptedException {
        var queue = new LinkedBlockingQueue<String>();

        int total = 10;

        var m = Set.of("message-5", "message-7");
        subscriber.subscribeBatchAsync(TOPIC_9, "test", String.class, null, (li) -> {
            System.out.println("列表：" + li);
            System.out.println("列表长度：" + li.size());
            var rl = li.stream()
                    .peek(queue::offer)
                    .filter(LambdaUtil.contains(m))
                    .collect(Collectors.toList());
            System.out.println("列表重试：" + rl);
            return CompletableFuture.completedFuture(rl);
        }, total, PubSubUtil.attemptLimit(100, 1), 100);

        for (int i = 0; i < total; i++) {
            publisher.publish(TOPIC_9, "message-" + i);
        }

        for (int i = 0; i < total; i++) {
            var s = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(s);
        }
        for (int i = 0; i < (total - m.size()); i++) {
            var s = queue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(s);
            Assert.assertFalse(m.contains(s));
        }

    }

    @Test
    public void serializeJSON() throws InterruptedException {
        final ObjectMapper jackson = new ObjectMapper();
        final var queue = new ToQueue<Book>();

        subscriber.subscribe(TOPIC_10, "c", Book.class, jackson::readValue, queue);

        var b = new Book();
        b.id = ThreadLocalRandom.current().nextInt(1000, 99999);
        b.name = "C++ Primary";
        var publisher = EncoderPublisher.wrapEncoder(this.publisher, jackson::writeValue);
        publisher.publish(TOPIC_10, b);

        Assert.assertEquals(b, queue.take());
        Assert.assertEquals(1, queue.getAsInt());
    }


    @SneakyThrows
    private static void waitEmpty(LinkedBlockingQueue<?> q) {
        while (true) {
            if (null == q.poll(1, TimeUnit.SECONDS)) break;
        }
    }

    //

    @Data
    static class Book {
        private int id;
        private String name;
    }

    static class ToQueue<T> extends LinkedBlockingQueue<T>
            implements Consumer<T>, IntSupplier {
        private final int timeout;
        private final AtomicInteger count = new AtomicInteger(0);

        ToQueue(int timeout) {
            this.timeout = timeout;
        }

        ToQueue() {
            this(0);
        }

        @SneakyThrows
        @Override
        public void accept(T t) {
            count.incrementAndGet();
            offer(t);
            log.debug("recv msg: {}", t);
            if (timeout > 0) {
                Thread.sleep(timeout);
            }
        }

        @Override
        public int getAsInt() {
            return count.get();
        }
    }

}
