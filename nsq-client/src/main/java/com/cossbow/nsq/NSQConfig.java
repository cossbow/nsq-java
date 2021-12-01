package com.cossbow.nsq;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

@Slf4j
public class NSQConfig {


    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}

    // for connect
    private String clientId;
    private String hostname;
    private boolean featureNegotiation = true;
    private Integer heartbeatInterval = null;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeout = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private String userAgent = null;
    private Integer msgTimeout = null;
    // for prepare
    private Integer maxInFlight = null;
    // for net
    private SslContext sslContext = null;
    private EventLoopGroup eventLoopGroup = null;
    // for consumer
    private boolean autoTouch;
    // for producer
    private int poolSize = 64;

    public NSQConfig() {
        try {
            clientId = InetAddress.getLocalHost().getHostName();
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
            userAgent = "nsqj";
        } catch (UnknownHostException e) {
            log.error("Local host name could not resolved", e);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isFeatureNegotiation() {
        return featureNegotiation;
    }

    public void setFeatureNegotiation(final boolean featureNegotiation) {
        this.featureNegotiation = featureNegotiation;
    }

    public Integer getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(final Integer heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Integer getOutputBufferSize() {
        return outputBufferSize;
    }

    public NSQConfig setMaxInFlight(final int maxInFlight) {
        this.maxInFlight = maxInFlight;
        return this;
    }

    public Integer getMaxInFlight() {
        return maxInFlight;
    }

    public void setOutputBufferSize(final Integer outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    public Integer getOutputBufferTimeout() {
        return outputBufferTimeout;
    }

    public void setOutputBufferTimeout(final Integer outputBufferTimeout) {
        this.outputBufferTimeout = outputBufferTimeout;
    }

    public boolean isTlsV1() {
        return tlsV1;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    public Integer getDeflateLevel() {
        return deflateLevel;
    }

    public void setDeflateLevel(final Integer deflateLevel) {
        this.deflateLevel = deflateLevel;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(final Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(final String userAgent) {
        this.userAgent = userAgent;
    }

    public Integer getMsgTimeout() {
        return msgTimeout;
    }

    public void setMsgTimeout(final Integer msgTimeout) {
        this.msgTimeout = msgTimeout;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        Objects.requireNonNull(sslContext);
        tlsV1 = true;
        this.sslContext = sslContext;
    }

    public EventLoopGroup getEventLoopGroup(EventLoopGroup defaultGroup) {
        return null != eventLoopGroup ? eventLoopGroup : defaultGroup;
    }

    public void setEventLoopGroup(final EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public boolean isAutoTouch() {
        return autoTouch;
    }

    public void setAutoTouch(boolean autoTouch) {
        this.autoTouch = autoTouch;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    //

    public void write(OutputStream os) throws IOException {
        var writer = new OutputStreamWriter(os);

        writer.append("{\"client_id\":\"").append(clientId).append("\", ");
        writer.append("\"hostname\":\"").append(hostname).append("\", ");
        writer.append("\"feature_negotiation\": true, ");
        if (getHeartbeatInterval() != null) {
            writer.append("\"heartbeat_interval\":").append(getHeartbeatInterval().toString()).append(", ");
        }
        if (getOutputBufferSize() != null) {
            writer.append("\"output_buffer_size\":").append(getOutputBufferSize().toString()).append(", ");
        }
        if (getOutputBufferTimeout() != null) {
            writer.append("\"output_buffer_timeout\":").append(getOutputBufferTimeout().toString()).append(", ");
        }
        if (isTlsV1()) {
            writer.append("\"tls_v1\":").append(String.valueOf(isTlsV1())).append(", ");
        }
        if (getCompression() == Compression.SNAPPY) {
            writer.append("\"snappy\": true, ");
        }
        if (getCompression() == Compression.DEFLATE) {
            writer.append("\"deflate\": true, ");
        }
        if (getDeflateLevel() != null) {
            writer.append("\"deflate_level\":").append(getDeflateLevel().toString()).append(", ");
        }
        if (getSampleRate() != null) {
            writer.append("\"sample_rate\":").append(getSampleRate().toString()).append(", ");
        }
        if (getMsgTimeout() != null) {
            writer.append("\"msg_timeout\":").append(getMsgTimeout().toString()).append(", ");
        }
        writer.append("\"user_agent\": \"").append(userAgent).append("\"}");

        writer.close();
    }

    //

    @Override
    public String toString() {
        var buffer = new ByteArrayOutputStream();
        try {
            write(buffer);
        } catch (IOException ignore) {
        }
        return buffer.toString();
    }
}
