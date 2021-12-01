package com.cossbow.nsq.frames;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

public abstract class NSQFrame {

    private int size;
    private ByteBuf buf;

    protected volatile transient String string = null;


    public static NSQFrame instance(int type) {
        switch (type) {
            case 0:
                return new ResponseFrame();
            case 1:
                return new ErrorFrame();
            case 2:
                return new MessageFrame();
        }
        return null;
    }


    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    public void setBuf(ByteBuf buf) {
        this.buf = buf;
        readData();
    }

    public String readData() {
        var s = string;
        if (null == s) {
            synchronized (this) {
                s = string;
                if (null == s) {
                    var b = buf;
                    if (null != b && b.isReadable()) {
                        try {
                            string = s = b.toString(CharsetUtil.UTF_8);
                        } finally {
                            release();
                        }
                    }
                }
            }
        }
        return s;
    }

    public void release() {
        ReferenceCountUtil.safeRelease(buf);
    }
}
