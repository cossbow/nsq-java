package com.cossbow.nsq.netty;

import com.cossbow.nsq.NSQCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class NSQEncoder extends MessageToMessageEncoder<NSQCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NSQCommand message, List<Object> out) throws Exception {
        var buf = message.makeBuf();
        out.add(buf);
    }
}
