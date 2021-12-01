package com.cossbow.nsq;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.ScheduledFuture;

public class NSQMessage<T> {

    private CharSequence id;
    private int attempts;
    private long timestamp;

    private ByteBuf buf;

    private final com.cossbow.nsq.Connection<T> connection;

    private volatile ScheduledFuture<?> touchFuture;
    private volatile ByteBufInputStream body;
    private volatile boolean hasRead;
    private volatile T obj;

    public NSQMessage(com.cossbow.nsq.Connection<T> connection) {
        this.connection = connection;
    }

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        cancelTouchFuture();
        connection.command(com.cossbow.nsq.NSQCommand.finish(this.id));
    }

    public void touch() {
        connection.command(com.cossbow.nsq.NSQCommand.touch(this.id));
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        cancelTouchFuture();
        connection.command(com.cossbow.nsq.NSQCommand.requeue(this.id, timeoutMillis));
    }

    public void requeue() {
        requeue(0);
    }

    private synchronized void cancelTouchFuture() {
        var f = touchFuture;
        touchFuture = null;
        if (null == f) return;
        f.cancel(false);
    }

    //


    public CharSequence getId() {
        return id;
    }

    void setId(CharSequence id) {
        this.id = id;
    }

    public int getAttempts() {
        return attempts;
    }

    void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getTimestamp() {
        return timestamp;
    }

    void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    void setNanoseconds(long nanoseconds) {
        this.timestamp = Math.floorDiv(nanoseconds, 1000_000L);
    }

    void setBuf(ByteBuf buf) {
        this.buf = buf;
    }


    public void setObj(T obj) {
        this.obj = obj;
    }

    public T getObj() {
        return obj;
    }

    public void setTouchFuture(ScheduledFuture<?> touchFuture) {
        this.touchFuture = touchFuture;
    }

    @Override
    public String toString() {
        return "NSQMessage{" +
                "id=" + id +
                ", attempts=" + attempts +
                '}';
    }

    //

    public synchronized ByteBufInputStream newMessageStream() {
        if (!hasRead) {
            try {
                body = new ByteBufInputStream(buf, true) {
                    @Override
                    public void close() {
                        try {
                            super.close();
                        } catch (Throwable ignore) {
                        }
                    }
                };
            } finally {
                hasRead = true;
            }
        }
        return body;
    }

    synchronized void release() {
        if (!hasRead) {
            try {
                ReferenceCountUtil.safeRelease(buf);
            } finally {
                hasRead = true;
            }
        }
    }

}
