package com.cossbow.nsq;

import com.cossbow.nsq.util.FutureUtil;
import com.cossbow.nsq.util.LambdaUtil;
import com.cossbow.nsq.exceptions.NSQException;
import com.cossbow.nsq.frames.ErrorFrame;
import com.cossbow.nsq.frames.MessageFrame;
import com.cossbow.nsq.frames.NSQFrame;
import com.cossbow.nsq.frames.ResponseFrame;
import com.cossbow.nsq.netty.NSQClientInitializer;
import com.cossbow.nsq.util.NSQUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class Connection<T> {
    private static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection<?>> STATE = AttributeKey.valueOf("Connection.state");
    private static volatile EventLoopGroup defaultGroup;

    static final int AvailableGroupType_Nio = 1;
    static final int AvailableNioType_Epoll = 2;
    static final int AvailableGroupType_KQueue = 3;
    private static final int AvailableGroupType;
    private static final Class<? extends Channel> SocketChannelClass;

    public static final long HEARTBEAT_MAX_INTERVAL = 60L * 1000L; //default one minute

    private static final com.cossbow.nsq.NSQCommand NOP = com.cossbow.nsq.NSQCommand.nop();


    static {
        if (isEpollIsAvailable()) {
            AvailableGroupType = AvailableNioType_Epoll;
            SocketChannelClass = EpollSocketChannel.class;
        } else if (isKQueueAvailable()) {
            AvailableGroupType = AvailableGroupType_KQueue;
            SocketChannelClass = KQueueSocketChannel.class;
        } else {
            AvailableGroupType = AvailableGroupType_Nio;
            SocketChannelClass = NioSocketChannel.class;
        }
    }

    private final ServerAddress address;
    private volatile Channel channel;
    private final Consumer<NSQMessage<T>> consumer;
    private final Consumer<NSQException> errorCallback;

    private final AtomicReference<CompletableFuture<NSQFrame>> responseFuture = new AtomicReference<>();

    private final NSQConfig config;
    private final boolean autoTouch;
    private int commandRetries = 6;
    private volatile int messageTimeout;

    private final AtomicReference<Long> lastHeartbeatSuccess = new AtomicReference<>(System.currentTimeMillis());


    Connection(ServerAddress serverAddress, NSQConfig config,
               Consumer<NSQMessage<T>> consumer,
               Consumer<NSQException> errorCallback) {
        this.address = serverAddress;
        this.config = config;
        this.consumer = consumer;
        this.errorCallback = errorCallback;
        this.autoTouch = config.isAutoTouch();
    }

    private CompletableFuture<Channel> doConnect() {
        try {
            final var bootstrap = new Bootstrap();
            var group = config.getEventLoopGroup(getDefaultGroup());
            bootstrap.group(group);
            bootstrap.channel(SocketChannelClass);
            bootstrap.handler(new NSQClientInitializer());
            // Start the connection attempt.
            final var cf = bootstrap.connect(
                    new InetSocketAddress(address.getHost(), address.getPort()));

            // Wait until the connection attempt succeeds or fails.

            var future = new CompletableFuture<Channel>();
            cf.addListener(f -> {
                if (f.isSuccess()) {
                    log.debug("[{}]connect {} success", hashCode(), address);
                    channel = cf.channel();
                    future.complete(channel);
                } else {
                    var e = cf.cause();
                    future.completeExceptionally(e);
                    log.warn("[{}]connect {} error", hashCode(), address, e);
                }
            });
            return future;
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Connection<T>> ready(Channel channel) {
        try {
            log.debug("[{}]ready: {}", hashCode(), address);
            channel.attr(STATE).set(this);
            var buf = Unpooled.wrappedBuffer(MAGIC_PROTOCOL_VERSION);
            try {
                channel.writeAndFlush(buf);
            } catch (Throwable e) {
                ReferenceCountUtil.safeRelease(buf);
                throw e;
            }

            return sendAndRecv(com.cossbow.nsq.NSQCommand.identify(config::write)).thenApply(frame -> {
                log.debug("[{}]identify reply: {}", hashCode(), frame.readData());
                var resp = NSQUtil.fromJson(frame.readData(), NSQConfigResponse.class);
                this.messageTimeout = resp.getMsgTimeout();
                return this;
            });
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public static <T> CompletableFuture<Connection<T>> connect(
            ServerAddress serverAddress, NSQConfig config,
            Consumer<NSQMessage<T>> consumer,
            Consumer<NSQException> errorCallback) {
        var cf = CompletableFuture.supplyAsync(() -> new Connection<>(
                serverAddress, config, consumer, errorCallback), getDefaultGroup());
        return cf.thenCompose(conn -> conn.doConnect().thenCompose(conn::ready));
    }

    private static EventLoopGroup getDefaultGroup() {
        var group = defaultGroup;
        if (group != null) return group;

        synchronized (MAGIC_PROTOCOL_VERSION) {
            group = defaultGroup;
            if (group != null) return group;

            var nt = System.getProperty("com.cossbow.nsq.Connection.EventLoopThreads");
            int n = 1;
            if (null != nt) {
                try {
                    n = Integer.parseInt(nt);
                } catch (NumberFormatException e) {
                    log.error("EventThreads must be an integer");
                }
                if (n < 1) {
                    log.error("EventThreads must be positive");
                    n = 1;
                }
            }

            var tf = new DefaultThreadFactory("nsq-group");
            switch (AvailableGroupType) {
                case AvailableNioType_Epoll:
                    group = new EpollEventLoopGroup(n, tf);
                    break;
                case AvailableGroupType_KQueue:
                    group = new KQueueEventLoopGroup(n, tf);
                    break;
                default:
                    group = new NioEventLoopGroup(n, tf);
                    break;
            }

            return defaultGroup = group;
        }
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public boolean isHeartbeatStatusOK() {
        return System.currentTimeMillis() - lastHeartbeatSuccess.get() <= HEARTBEAT_MAX_INTERVAL;
    }

    public boolean isHealthy() {
        return isConnected() && isHeartbeatStatusOK();
    }

    public void incoming(final NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(frame.readData())) {
                heartbeat();
            } else {
                var future = responseFuture.getAndSet(null);
                if (null != future) {
                    future.complete(frame);
                }
            }
            return;
        }

        if (frame instanceof ErrorFrame) {
            var ex = NSQException.of((ErrorFrame) frame);
            var future = responseFuture.getAndSet(null);
            if (null != future) {
                future.completeExceptionally(ex);
            }
            if (errorCallback != null) {
                errorCallback.accept(ex);
            }
            return;
        }

        if (frame instanceof MessageFrame) {
            final var msg = (MessageFrame) frame;

            final var message = new NSQMessage<>(this);
            message.setAttempts(msg.getAttempts());
            message.setId(msg.getId());
            message.setTimestamp(msg.getTimestamp());
            message.setBuf(msg.getBuf());
            message.setNanoseconds(msg.getTimestamp());
            if (autoTouch) {
                int timeout = messageTimeout;
                if (timeout > 1000) {
                    timeout -= 1000;
                } else {
                    timeout >>= 1;
                }
                message.setTouchFuture(NSQUtil.SCHEDULER.scheduleWithFixedDelay(
                        message::touch, timeout, timeout, TimeUnit.MILLISECONDS));
            }
            consumer.accept(message);
            return;
        }
        log.warn("Unknown frame type: " + frame);
    }


    private void heartbeat() {
        log.trace("HEARTBEAT!");
        command(NOP);
        lastHeartbeatSuccess.getAndSet(System.currentTimeMillis());
    }

    public void close() {
        log.info("Closing  connection: " + this);
        channel.disconnect();
    }

    public CompletableFuture<Void> closeAsync() {
        return NSQUtil.future(channel.disconnect(), LambdaUtil.empty());
    }

    public ChannelFuture command(final com.cossbow.nsq.NSQCommand command) {
        return channel.writeAndFlush(command);
    }

    public CompletableFuture<Void> send(com.cossbow.nsq.NSQCommand command) {
        return NSQUtil.future(command(command), LambdaUtil.empty());
    }

    private CompletableFuture<NSQFrame> sendAndRecv0(com.cossbow.nsq.NSQCommand command) {
        var future = new CompletableFuture<NSQFrame>();
        if (!responseFuture.compareAndSet(null, future)) {
            return CompletableFuture.failedFuture(
                    new RetryException());
        }

        var cf = command(command);
        NSQUtil.SCHEDULER.schedule(() -> {
            cf.cancel(false);
            var f = responseFuture.getAndSet(null);
            if (null == f) return;
            f.completeExceptionally(
                    new TimeoutException("command: " + command + " timeout"));
        }, 15, TimeUnit.SECONDS);

        return future;
    }

    public CompletableFuture<NSQFrame> sendAndRecv(com.cossbow.nsq.NSQCommand command) {
        return FutureUtil.retry(() -> sendAndRecv0(command),
                commandRetries, Duration.ZERO,
                e -> e instanceof RetryException);
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }

    public int getCommandRetries() {
        return commandRetries;
    }

    public void setCommandRetries(int commandRetries) {
        this.commandRetries = commandRetries;
    }

    //

    private static boolean isEpollIsAvailable() {
        try {
            Class.forName("io.netty.channel.epoll.Epoll");
            return Epoll.isAvailable();
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private static boolean isKQueueAvailable() {
        try {
            Class.forName("io.netty.channel.kqueue.KQueue");
            return KQueue.isAvailable();
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }


    //

    static class RetryException extends Exception {
        private static final long serialVersionUID = 1953168381411364799L;

        public RetryException(String message) {
            super(message, null, false, false);
        }

        public RetryException() {
            this(null);
        }
    }

}
