package com.cossbow.nsq.util;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public
interface Decoder {

    /**
     * @param is
     * @param type
     * @param <T>
     * @return
     * @throws IOException
     */
    <IS extends InputStream, T> T decode(IS is, Class<T> type) throws IOException;

    /**
     * @param s
     * @param type
     * @param <T>
     * @return
     */
    default <T> T decode(byte[] s, Class<T> type) throws IOException {
        return decode(new ByteArrayInputStream(s), type);
    }

}
