package com.cossbow.nsq;

import com.cossbow.nsq.lookup.DefaultNSQLookup;
import com.cossbow.nsq.lookup.NSQLookup;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class NsqBaseTest {
    static {
        System.setProperty("logging.config", "classpath:log4j.xml");
    }

    final HttpClient client = HttpClient.newBuilder().build();

    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);

    final String lookupAddress = "127.0.0.1:4161";
    final NSQLookup nsqLookup = new DefaultNSQLookup();

    {
        var uri = URI.create("http://" + lookupAddress);
        nsqLookup.addLookupAddress(uri.getHost(), uri.getPort());
    }


    final NsqPublisherImpl publisher = publisher();

    final NsqSubscriberImpl subscriber = subscriber();

    @After
    public void after() {
        subscriber.unSubscribeAll().join();
    }


    public NsqPublisherImpl publisher() {
        var addresses = nsqLookup.lookupNodeAsync().join();
        return new NsqPublisherImpl(addresses, 10);
    }


    public NsqSubscriberImpl subscriber() {
        return new NsqSubscriberImpl(
                nsqLookup,
                60_000,
                3,
                1000,
                10,
                100,
                "junit", false);
    }

    protected News create(int id) {
        News news = new News();
        news.setId(id);
        news.setTitle("测试新闻标题" + id);
        news.setContent("就这么点内容！你看着办！" + id);
        news.setCreatedTime(Instant.now());
        news.setPublishedTime(OffsetDateTime.now());
        return news;
    }

    @Test
    public void serial() throws IOException {
        var news = create(1001);
        var data = PubSubUtil.getDefaultEncoder().encode(news);
        System.out.println(new String(data));
        var n2 = PubSubUtil.getDefaultDecoder().decode(data, News.class);
        System.out.println(n2);
    }


    protected CompletableFuture<String> get(String url) {
        var req = HttpRequest.newBuilder(URI.create(url)).build();
        var f = client.sendAsync(req, HttpResponse.BodyHandlers.ofString());
        return f.thenApply(HttpResponse::body);
    }

    protected long limitAttemps(int attempts, int attemptsLimit) {
        if (attempts < attemptsLimit) {
            return 200;
        } else {
            return 0;
        }
    }
}
