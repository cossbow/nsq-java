package com.cossbow.nsq;

import com.cossbow.nsq.lookup.DefaultNSQLookup;
import com.cossbow.nsq.lookup.NSQLookup;
import org.junit.Test;

public class Nsq {

    public static ServerAddress getNsqdHost() {
        String hostName = System.getenv("NSQD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return new ServerAddress(hostName,4150);
    }

    public static String getNsqLookupdHost() {
        String hostName = System.getenv("NSQLOOKUPD_HOST");
        if (hostName == null) {
            hostName = "localhost";
        }
        return hostName;
    }

    @Test
    public void lookup() {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        lookup.lookupNodeAsync().thenAccept(serverAddresses -> {
            for (var sa : serverAddresses) {
                System.out.println(sa);
            }
        }).join();

    }


    public static final NSQConfig CONFIG = new NSQConfig();

}
