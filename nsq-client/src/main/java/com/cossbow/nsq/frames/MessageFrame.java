package com.cossbow.nsq.frames;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class MessageFrame extends NSQFrame {
    private static final short MESSAGE_ID_SIZE = 16;

    private long timestamp;
    private int attempts;
    private CharSequence id;

    private ByteBuf messageBodyBuf;


    public long getTimestamp() {
        return timestamp;
    }

    public int getAttempts() {
        return attempts;
    }


    public CharSequence getId() {
        return id;
    }

    public ByteBuf getBuf() {
        return messageBodyBuf;
    }

    @Override
    public void setBuf(ByteBuf buf) {
        try {
            timestamp = buf.readLong();
            attempts = buf.readShort();
            id = buf.readCharSequence(MESSAGE_ID_SIZE, StandardCharsets.US_ASCII);
            messageBodyBuf = buf.readBytes(buf.readableBytes());
        } finally {
            ReferenceCountUtil.safeRelease(buf);
        }
    }

    @Override
    public String readData() {
        return "MESSAGE: " + id;
    }

}
