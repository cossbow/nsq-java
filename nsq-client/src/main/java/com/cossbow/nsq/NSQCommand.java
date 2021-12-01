package com.cossbow.nsq;

import com.cossbow.nsq.util.ThrowoutConsumer;
import com.cossbow.nsq.exceptions.NSQDException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Stream;

public class NSQCommand {

    private enum Type {
        TYPE_OPERATE,
        TYPE_DATA,
        TYPE_DATA_LIST,
        TYPE_WRITE,
        TYPE_WRITE_LIST,
        ;
    }

    //

    private final String line;

    private final Type type;
    private final int estimateSize;
    private final byte[] data;
    private final Collection<byte[]> datas;
    private final ThrowoutConsumer<ByteBufOutputStream, IOException> writer;
    private final Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> writers;


    //


    private NSQCommand(String line, Type type, byte[] data, Collection<byte[]> datas,
                       ThrowoutConsumer<ByteBufOutputStream, IOException> writer,
                       Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> writers) {
        assert line.charAt(line.length() - 1) != '\n';
        //
        this.line = line;
        this.type = type;
        this.data = data;
        this.datas = datas;
        this.writer = writer;
        this.writers = writers;
        //
        int lineSize = line.length() << 1 + 1;
        switch (type) {
            case TYPE_DATA:
                estimateSize = lineSize + data.length;
                break;
            case TYPE_DATA_LIST:
                for (var d : datas) {
                    lineSize += d.length;
                }
                estimateSize = lineSize;
                break;
            default:
                estimateSize = lineSize + 64;
        }
    }

    private NSQCommand(String line) {
        this(line, Type.TYPE_OPERATE, null, null, null, null);
    }

    private NSQCommand(String line, byte[] data) {
        this(line, Type.TYPE_DATA, data, null, null, null);
    }

    private NSQCommand(String line, Collection<byte[]> data) {
        this(line, Type.TYPE_DATA_LIST, null, data, null, null);
    }

    private NSQCommand(String line, ThrowoutConsumer<ByteBufOutputStream, IOException> writer) {
        this(line, Type.TYPE_WRITE, null, null, writer, null);
    }

    private NSQCommand(String line, Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> writers) {
        this(line, Type.TYPE_WRITE_LIST, null, null, null, writers);
    }

    //

    private void init(ByteBuf buf) {
        buf.writeCharSequence(line, StandardCharsets.UTF_8);
        buf.writeByte('\n');
    }

    private void writeData(ByteBuf buf, byte[] data) {
        buf.writeInt(data.length);
        buf.writeBytes(data);
    }

    private void writeData(ByteBuf buf, Collection<byte[]> data) {
        //for MPUB messages.
        if (data.size() > 1) {
            //write total bodysize and message size
            int bodySize = 4; //4 for total messages int.
            for (var d : data) {
                bodySize += 4; //message size
                bodySize += d.length;
            }
            buf.writeInt(bodySize);
            buf.writeInt(data.size());
        }

        for (var d : data) {
            buf.writeInt(d.length);
            buf.writeBytes(d);
        }
    }

    private void writeCallback(
            ByteBuf buf,
            ThrowoutConsumer<ByteBufOutputStream, IOException> writer) {
        int startIndex = buf.writerIndex();

        buf.writeInt(0);

        var os = new ByteBufOutputStream(buf);
        try {
            writer.accept(os);
        } catch (IOException e) {
            throw new NSQDException(e);
        }
        var end = buf.writerIndex(); // 结束位置
        var len = os.writtenBytes();    // 数据长度

        buf.writerIndex(startIndex);        // 写入长度
        buf.writeInt(len);
        buf.writerIndex(end);

    }

    private void writeCallbackStream(
            ByteBuf buf,
            Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> writers) {
        int startIndex = buf.writerIndex();

        buf.writeInt(0);            // 总大小
        buf.writeInt(0);            // 总数量

        var lenArr = writers.mapToInt(writer -> {
            var is = buf.writerIndex(); // size位置
            buf.writeInt(0);

            var os = new ByteBufOutputStream(buf);
            try {
                writer.accept(os);
            } catch (IOException e) {
                throw new NSQDException(e);
            }
            var ie = buf.writerIndex(); // 结束位置
            var len = os.writtenBytes();

            buf.writerIndex(is);
            buf.writeInt(len);
            buf.writerIndex(ie);

            return len + 4;
        }).toArray();

        var end = buf.writerIndex();
        buf.writerIndex(startIndex);            // 写入长度
        int len = 4;
        for (var it : lenArr) {
            len += it;
        }
        buf.writeInt(len);              // 总大小
        buf.writeInt(lenArr.length);    // 总数量
        buf.writerIndex(end);
    }

    //

    public ByteBuf makeBuf() {
        var buf = ByteBufAllocator.DEFAULT.buffer(estimateSize);
        try {
            init(buf);
            switch (type) {
                case TYPE_OPERATE:
                    break;
                case TYPE_DATA:
                    writeData(buf, data);
                    break;
                case TYPE_DATA_LIST:
                    writeData(buf, datas);
                    break;
                case TYPE_WRITE:
                    writeCallback(buf, writer);
                    break;
                case TYPE_WRITE_LIST:
                    writeCallbackStream(buf, writers);
                    break;
                default:
                    // 不可能的分支
            }
            return buf;
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw e;
        }
    }

    @Override
    public String toString() {
        return line;
    }


    // Identify creates a new Command to provide information about the client.  After connecting,
    // it is generally the first message sent.
    //
    // The supplied body should be a map marshaled into JSON to provide some flexibility
    // for this command to evolve over time.
    //
    // See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
    // on the supported options
    public static NSQCommand identify(byte[] body) {
        return new NSQCommand("IDENTIFY", body);
    }

    public static NSQCommand identify(ThrowoutConsumer<ByteBufOutputStream, IOException> writer) {
        return new NSQCommand("IDENTIFY", writer);
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static NSQCommand touch(CharSequence messageID) {
        return new NSQCommand("TOUCH " + messageID);
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static NSQCommand finish(CharSequence messageID) {
        return new NSQCommand("FIN " + messageID);
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static NSQCommand subscribe(String topic, String channel) {
        return new NSQCommand("SUB " + topic + " " + channel);
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static NSQCommand startClose() {
        return new NSQCommand("CLS");
    }

    public static NSQCommand requeue(CharSequence messageID, int timeoutMillis) {
        return new NSQCommand("REQ " + messageID + " " + timeoutMillis);
    }

    // Nop creates a new Command that has no effect server side.
    // Commonly used to respond to heartbeats
    public static NSQCommand nop() {
        return new NSQCommand("NOP");
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static NSQCommand ready(int rdy) {
        return new NSQCommand("RDY " + rdy);
    }

    // Publish creates a new Command to write a message to a given topic
    public static NSQCommand publish(String topic, byte[] message) {
        return new NSQCommand("PUB " + topic, message);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand publish(String topic, ThrowoutConsumer<ByteBufOutputStream, IOException> writer) {
        return new NSQCommand("PUB " + topic, writer);
    }

    // Publish creates a new Command to write a deferred message to a given topic
    public static NSQCommand publish(String topic, int deferTime, byte[] message) {
        return new NSQCommand("DPUB " + topic + " " + deferTime, message);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand publish(String topic, int deferTime, ThrowoutConsumer<ByteBufOutputStream, IOException> writer) {
        return new NSQCommand("DPUB " + topic + " " + deferTime, writer);
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static NSQCommand multiPublish(String topic, Collection<byte[]> bodies) {
        return new NSQCommand("MPUB " + topic, bodies);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand multiPublish(String topic, Stream<ThrowoutConsumer<ByteBufOutputStream, IOException>> writers) {
        return new NSQCommand("MPUB " + topic, writers);
    }

}
