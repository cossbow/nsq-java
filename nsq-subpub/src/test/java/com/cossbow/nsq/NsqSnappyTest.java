package com.cossbow.nsq;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;


public class NsqSnappyTest extends NsqBaseTest {
    static {
        System.setProperty("mq.nsq.snappy", "true");
    }

    private News create() {
        return create(ThreadLocalRandom.current().nextInt(1000, 99999));
    }

    @Test
    public void testCoder() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-snappy-coder-1";
        subscriber.subscribe(topic, "ch", News.class, news -> {
            System.out.println(news);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        publisher.publish(topic, create()).join();

        synchronized (syncer) {
            syncer.wait();
        }
    }

    @Test
    public void testBytes() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-snappy-bytes-1";
        subscriber.subscribe(topic, "ch", PubSubUtil.stringDecoder(), s -> {
            System.out.println(s);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        var v = create().toString();
        publisher.publish(topic, v).join();

        synchronized (syncer) {
            syncer.wait();
        }
    }


}
