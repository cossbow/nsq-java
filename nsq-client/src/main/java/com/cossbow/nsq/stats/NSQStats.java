package com.cossbow.nsq.stats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;

@Data
public class NSQStats {

//    private String version;

    private String health;

    private long startTime;

    private TopicStats[] topics;

//    private MemoryStats memory;

    @JsonIgnore
    private String address;

    //

    public boolean isHealth() {
        return "OK".equals(health);
    }

    //
    //
    //
    @Data
    public static class TopicStats {

        private String topicName;
        private List<ChannelStats> channels;

        private int depth;
        private int backendDepth;

        private int messageCount;
        private boolean paused;

//        private E2eProcessingLatencyStats e2eProcessingLatency;

        public void merge(TopicStats o) {
            depth += o.depth;
            backendDepth += o.backendDepth;

            for (var ch : channels) {
                for (var ch1 : o.channels) {
                    if (null == ch.channelName) continue;
                    if (ch.channelName.equals(ch1.channelName)) {
                        ch.merge(ch1);
                    }
                }
            }
        }
    }

    @Data
    public static class ChannelStats {

        private String channelName;

        private int depth;
        private int backendDepth;

        private int inFlightCount;
        private int deferredCount;
        private int requeueCount;
        private int timeoutCount;

        private int messageCount;

//        private List<ClientStats> clients;
//        private boolean paused;
//
//        private E2eProcessingLatencyStats e2eProcessingLatency;

        public void merge(ChannelStats o) {
            depth += o.depth;
            backendDepth += o.backendDepth;
            inFlightCount += o.inFlightCount;
            deferredCount += o.deferredCount;
            messageCount += o.messageCount;
            requeueCount += o.requeueCount;
            timeoutCount += o.timeoutCount;
        }

        public int untreated() {
            return depth + inFlightCount + deferredCount;
        }
    }

    @Data
    public static class E2eProcessingLatencyStats {

        private int count;

        private String percentiles;
    }

    @Data
    public static class ClientStats {

        private String clientId;
        private String hostname;
        private String version;

        private String remoteAddress;
        private int state;

        private int readyCount;

        private int inFlightCount;

        private int messageCount;

        private int finishCount;

        private int requeueCount;

        private int connectTs;

        private int sampleRate;
        private boolean deflate;
        private boolean snappy;

        private String userAgent;
        private boolean tls;

        private String tlsCipherSuite;

        private String tlsVersion;

        private String tlsNegotiatedProtocol;

        private boolean tlsNegotiatedProtocolIsMutual;
    }

    @Data
    public static class MemoryStats {

        private int heapObjects;

        private int heapIdleBytes;

        private int heapInUseBytes;

        private int heapReleasedBytes;

        private int gcPauseUsec100;

        private int gcPauseUsec99;

        private int gcPauseUsec95;

        private int nextGcBytes;

        private int gcTotalRuns;
    }
}
