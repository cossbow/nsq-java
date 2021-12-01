package com.cossbow.nsq.lookup;

import com.cossbow.nsq.util.HttpStatusException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.cossbow.nsq.util.LambdaUtil;
import com.cossbow.nsq.util.NSQUtil;
import com.cossbow.nsq.ServerAddress;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class DefaultNSQLookup implements NSQLookup {
    private final Set<String> addresses = ConcurrentHashMap.newKeySet();


    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }


    @Override
    public Set<ServerAddress> lookup(String topic) {
        try {
            return lookupAsync(topic).join();
        } catch (CompletionException e) {
            log.error("lookup exception", e.getCause());
            return ConcurrentHashMap.newKeySet();
        }
    }

    private Flux<ServerAddress> lookupNodes(Flux<String> urls) {
        return urls.flatMap(this::lookupNodes);
    }

    private Flux<ServerAddress> lookupNodes(String url) {
        return NSQUtil.get(url, LookupResponse.class)
                .onErrorResume(HttpStatusException.class, e -> {
                    log.debug("lookup '{}' empty", url);
                    return Mono.empty();
                })
                .flatMapIterable(r -> r.producers)
                .map(LookupResponse.Producer::toServerAddress);
    }

    @Override
    public CompletableFuture<Set<ServerAddress>> lookupAsync(String topic) {
        var lookupAddresses = getLookupAddresses();
        String uri = "/lookup?topic=" + urlEncode(topic);
        var urls = Flux.fromIterable(lookupAddresses).map(addr -> addr + uri);
        return lookupNodes(urls).collect(Collectors.toSet()).flatMap(addresses -> {
            if (!addresses.isEmpty()) {
                return Mono.just(addresses);
            }
            return createTopic(topic);
        }).toFuture();
    }


    public Flux<ServerAddress> lookupNodes() {
        var lookupAddresses = getLookupAddresses();
        var urls = Flux.fromIterable(lookupAddresses).map(addr -> addr + "/nodes");
        return lookupNodes(urls);
    }

    public CompletableFuture<Set<ServerAddress>> lookupNodeAsync() {
        return lookupNodes().collect(Collectors.toSet()).toFuture();
    }

    private Mono<Set<ServerAddress>> createTopic(String topic) {
        log.debug("lookup create topic[{}]", topic);
        String uri = "/topic/create?topic=" + urlEncode(topic);
        return requestAll(uri);
    }

    public Mono<Set<ServerAddress>> deleteTopic(String topic) {
        String uri = "/topic/delete?topic=" + urlEncode(topic);
        return requestAll(uri);
    }

    private Mono<Set<ServerAddress>> requestAll(String uri) {
        return lookupNodes().flatMap(address -> {
            var url = address.httpAddress() + uri;
            return NSQUtil.post(url).map(LambdaUtil.just(address));
        }).collect(Collectors.toSet());
    }

    private static String urlEncode(String t) {
        return URLEncoder.encode(t, StandardCharsets.UTF_8);
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }


    @Data
    static class LookupResponse {

        private List<Producer> producers;

        @Data
        static class Producer {
            @JsonProperty("broadcast_address")
            private String broadcastAddress;
            @JsonProperty("tcp_port")
            private int tcpPort;
            @JsonProperty("http_port")
            private int httpPort;

            public ServerAddress toServerAddress() {
                return new ServerAddress(broadcastAddress, tcpPort, httpPort);
            }
        }

    }
}
