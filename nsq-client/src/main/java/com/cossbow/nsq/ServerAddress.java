package com.cossbow.nsq;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

public class ServerAddress {

    private String host;
    private int port;
    private int httpPort;

    @JsonIgnore
    private transient volatile String _string;

    public ServerAddress(final String host, final int port) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
    }

    public ServerAddress(final String host, final int port, final int httpPort) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.httpPort = httpPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String httpAddress() {
        return "http://" + host + ':' + httpPort;
    }

    public String toString() {
        var s = _string;
        if (null != s) return s;
        synchronized (this) {
            s = _string;
            if (null != s) return s;
            _string = s = host + ":" + port;
            return s;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerAddress that = (ServerAddress) o;

        if (port != that.port) return false;
        if (httpPort != that.httpPort) return false;
        return Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + httpPort;
        return result;
    }
}
