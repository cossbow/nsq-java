package com.cossbow.nsq.stats;

import com.cossbow.nsq.util.NSQUtil;
import com.cossbow.nsq.lookup.NSQLookup;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class NSQStatsMonitor {

    private final Set<String> nodeSet = ConcurrentHashMap.newKeySet();

    private final NSQLookup nsqLookup;

    private final Set<? extends Consumer<Map<String, NSQStats>>> handlers;

    private volatile boolean started;

    private volatile long lookupNodesPeriod;
    private volatile long queryStatsPeriod;

    /**
     * @param nsqLookup         NSQLookup bean
     * @param handlers          callback for stats
     * @param lookupNodesPeriod period of lookup nodes
     * @param queryStatsPeriod  period of query stats
     */
    public NSQStatsMonitor(@NonNull NSQLookup nsqLookup,
                           @NonNull Set<? extends Consumer<Map<String, NSQStats>>> handlers,
                           long lookupNodesPeriod,
                           long queryStatsPeriod) {
        if (lookupNodesPeriod <= 0) {
            throw new IllegalArgumentException("lookupNodesPeriod invalid");
        }
        if (queryStatsPeriod <= 0) {
            throw new IllegalArgumentException("lookupNodesPeriod invalid");
        }

        this.nsqLookup = nsqLookup;
        this.handlers = handlers;

        this.lookupNodesPeriod = lookupNodesPeriod;
        this.queryStatsPeriod = queryStatsPeriod;
    }

    public synchronized void start() {
        if (started) {
            throw new IllegalArgumentException("nsq stats monitor is started");
        }
        started = true;

        NSQUtil.SCHEDULER.scheduleAtFixedRate(this::lookupNodes, lookupNodesPeriod, lookupNodesPeriod, TimeUnit.MINUTES);
        NSQUtil.SCHEDULER.scheduleAtFixedRate(this::queryStatsLoop, queryStatsPeriod, queryStatsPeriod, TimeUnit.MINUTES);

        lookupNodes().thenAccept(v -> queryStatsLoop());

    }

    /**
     * 轮询所有节点
     */
    private CompletableFuture<Void> lookupNodes() {
        return nsqLookup.lookupNodeAsync().thenAccept(serverAddresses -> {
            log.debug("lookup nsq nodes: {}", serverAddresses);
            serverAddresses.forEach(serverAddress -> nodeSet.add(serverAddress.httpAddress()));
        }).exceptionally(e -> {
            log.error("lookup nsq nodes fail", e.getCause());
            return null;
        });
    }

    private Mono<Map<String, NSQStats>> queryStats() {
        if (nodeSet.isEmpty()) {
            return Mono.just(Map.of());
        }

        return Flux.merge(Flux.fromIterable(nodeSet).map(address ->
                queryStats(address + "/stats?format=json")))
                .collectMap(NSQStats::getAddress);
    }

    /**
     * 轮询每个节点状态
     */
    private void queryStatsLoop() {
        if (nodeSet.isEmpty()) {
            return;
        }

        queryStats().doOnError(e -> {
            log.error("query status error", e);
        }).subscribe((map) -> {
            for (var handler : handlers) handler.accept(map);
        });

    }

    private Mono<NSQStats> queryStats(String address) {
        return NSQUtil.get(address, NSQStats.class).map(ns -> {
            ns.setAddress(address);
            return ns;
        });
    }


    /**
     * 查询话题状态
     */
    public CompletableFuture<Map<String, NSQStats.TopicStats>> queryTopicStats(String... topics) {
        if (null == topics || topics.length == 0) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return queryStats().map(map -> {
            var topicSet = Set.of(topics);
            return map.values().stream()
                    .flatMap(v -> Arrays.stream(v.getTopics()))
                    .filter(ts -> topicSet.contains(ts.getTopicName()))
                    .collect(Collectors.toMap(NSQStats.TopicStats::getTopicName,
                            Function.identity(), (ts1, ts2) -> {
                                ts1.merge(ts2);
                                return ts1;
                            }));
        }).toFuture();
    }

}
