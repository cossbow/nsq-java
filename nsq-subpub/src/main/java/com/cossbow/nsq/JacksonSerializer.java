package com.cossbow.nsq;

import com.cossbow.nsq.util.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class JacksonSerializer implements Serializer {

    private static final Map<ObjectMapper, JacksonSerializer> INSTANCES =
            new ConcurrentHashMap<>();

    public static JacksonSerializer getInstance(ObjectMapper mapper) {
        return INSTANCES.computeIfAbsent(mapper, JacksonSerializer::new);
    }

    private final ObjectMapper mapper;

    private JacksonSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <T> void encode(OutputStream os, T v) throws IOException {
        mapper.writeValue(os, v);
    }

    @Override
    public <IS extends InputStream, T> T decode(IS is, Class<T> type) throws IOException {
        return mapper.readValue(is, type);
    }


    //

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JacksonSerializer that = (JacksonSerializer) o;
        return mapper.equals(that.mapper);
    }

    @Override
    public int hashCode() {
        return mapper.hashCode();
    }

}
