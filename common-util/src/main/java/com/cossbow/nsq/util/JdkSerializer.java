package com.cossbow.nsq.util;

import java.io.*;

public class JdkSerializer implements Serializer {

    public static final JdkSerializer INSTANCE = new JdkSerializer();

    private JdkSerializer() {
    }

    //

    @Override
    public <T> void encode(OutputStream os, T v) throws IOException {
        try (var oo = new ObjectOutputStream(os)) {
            oo.writeObject(v);
        }
    }

    @Override
    public <IS extends InputStream, T> T decode(IS is, Class<T> type) throws IOException {
        try (var in = new ObjectInputStream(is)) {
            @SuppressWarnings("unchecked")
            var t = (T) in.readObject();
            return t;
        } catch (ClassNotFoundException e) {
            throw new DecodingException(e);
        }
    }

}
