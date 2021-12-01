package com.cossbow.nsq.netty;

import com.cossbow.nsq.Connection;
import com.cossbow.nsq.frames.NSQFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        var connection = ctx.channel().attr(Connection.STATE).get();
        if (connection != null) {
            log.info("Channel disconnected! " + connection);
        } else {
            log.error("No connection set for : " + ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        log.error("NSQHandler exception caught", cause);

        ctx.channel().close();
        var con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            con.close();
        } else {
            log.warn("No connection set for : " + ctx.channel());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
        final var con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            ctx.channel().eventLoop().execute(() -> con.incoming(msg));
        } else {
            log.warn("No connection set for : " + ctx.channel());
        }
    }
}
