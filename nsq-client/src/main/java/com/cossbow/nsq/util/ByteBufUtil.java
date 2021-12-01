package com.cossbow.nsq.util;

import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

final
public class ByteBufUtil {
    private ByteBufUtil() {
    }


    public static Mono<ByteBuf> create(String data) {
        assert null != data;
        return Mono.create(sink -> {
            try {
                sink.success(io.netty.buffer.ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data));
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    public static Mono<ByteBuf> create(ThrowoutSupplier<String, ? extends IOException> supplier) {
        assert null != supplier;
        return Mono.create(sink -> {
            try {
                var data = supplier.get();
                sink.success(io.netty.buffer.ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data));
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    public static Mono<ByteBuf> create(byte[] bytes) {
        assert null != bytes;
        return Mono.create(sink -> {
            try {
                sink.success(Unpooled.wrappedBuffer(bytes));
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    public static Mono<ByteBuf> create(ThrowoutConsumer<OutputStream, IOException> writer) {
        assert null != writer;

        return Mono.create(ms -> {
            ByteBuf buf;
            try {
                buf = ByteBufAllocator.DEFAULT.buffer();
            } catch (Throwable e) {
                ms.error(e);
                return;
            }
            try (var out = new ByteBufOutputStream(buf)) {
                writer.accept(out);
                ms.success(buf);
            } catch (Throwable e) {
                ReferenceCountUtil.safeRelease(buf);
                ms.error(e);
            }
        });
    }

    public static Mono<ByteBuf> createDirect(Consumer<ByteBuf> writer) {
        assert null != writer;

        return Mono.create(ms -> {
            var buf = ByteBufAllocator.DEFAULT.buffer();
            try {
                writer.accept(buf);
                ms.success(buf);
            } catch (Throwable e) {
                ReferenceCountUtil.safeRelease(buf);
                ms.error(e);
            }
        });
    }


    //

    public static InputStream asInputStream(ByteBuf buf) {
        return new ByteBufInputStream(buf, true);
    }


    //

}
