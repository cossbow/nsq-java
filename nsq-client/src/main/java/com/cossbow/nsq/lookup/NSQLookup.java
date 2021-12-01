package com.cossbow.nsq.lookup;

import com.cossbow.nsq.ServerAddress;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface NSQLookup {

    Set<ServerAddress> lookup(String topic);

    CompletableFuture<Set<ServerAddress>> lookupAsync(String topic);

    CompletableFuture<Set<ServerAddress>> lookupNodeAsync();

    void addLookupAddress(String addr, int port);

}
