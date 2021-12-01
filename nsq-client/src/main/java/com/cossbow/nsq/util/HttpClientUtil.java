package com.cossbow.nsq.util;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.HttpResources;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static reactor.netty.NettyPipeline.OnChannelReadIdle;
import static reactor.netty.NettyPipeline.OnChannelWriteIdle;

final
public class HttpClientUtil {
    private HttpClientUtil() {
    }

    //

    public static final Consumer<HttpHeaders> HTTP_HEADERS_JSON = setHeaders(Map.of(
            "Content-Type", "application/json",
            "Accept", "application/json"
    ));
    public static final Consumer<HttpHeaders> HTTP_HEADERS_FORM = setHeaders(Map.of(
            "Content-Type", "application/x-www-form-urlencoded"
    ));

    public static final BiConsumer<HttpClientResponse, Connection> HTTP_OK_CHECKER
            = httpStatusChecker(200);

    //

    static final HttpClient DefaultClient;
    static final HttpClient StatusCheckedClient;

    static {
        try {
            var loop = LoopResources.create("HttpClient", 2,
                    LoopResources.DEFAULT_IO_WORKER_COUNT, true);
            ConnectionProvider provider = ConnectionProvider.builder("HttpClient")
                    .maxConnections(Integer.MAX_VALUE).build();
            var constructor = HttpResources.class.getDeclaredConstructor(
                    LoopResources.class, ConnectionProvider.class);
            constructor.setAccessible(true);
            var resources = constructor.newInstance(loop, provider);
            DefaultClient = HttpClient.create(resources).tcpConfiguration(
                    timeoutSecondsHandler(60, 60)).secure(spec -> {
                var builder = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE);
                spec.sslContext(builder);
            }).compress(true);
            StatusCheckedClient = DefaultClient.doOnResponse(HTTP_OK_CHECKER);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }

    }

    public static HttpClient client() {
        return DefaultClient;
    }

    public static HttpClient clientWithStatusCheck() {
        return StatusCheckedClient;
    }

    public static HttpClient clientWithStatusCheck(BiConsumer<HttpClientResponse, Connection> checker) {
        return client().doOnResponse(checker);
    }

    public static <K extends CharSequence, V extends CharSequence>
    Consumer<HttpHeaders> setHeaders(Map<K, V> data) {
        Objects.requireNonNull(data);
        int size = data.size();
        var keys = new CharSequence[size];
        var values = new CharSequence[size];
        var i = 0;
        for (var en : data.entrySet()) {
            keys[i] = en.getKey();
            values[i] = en.getValue();
            i++;
        }
        return httpHeaders -> {
            for (int j = 0; j < size; j++) {
                httpHeaders.set(keys[j], values[j]);
            }
        };
    }

    public static Function<TcpClient, TcpClient> timeoutMillisHandler(long writeTimeout, long readTimeout) {
        return timeoutHandler(writeTimeout, readTimeout, TimeUnit.MILLISECONDS);
    }

    public static Function<TcpClient, TcpClient> timeoutSecondsHandler(long writeTimeout, long readTimeout) {
        return timeoutHandler(writeTimeout, readTimeout, TimeUnit.SECONDS);
    }

    public static Function<TcpClient, TcpClient> timeoutHandler(long writeTimeout, long readTimeout, TimeUnit timeUnit) {
        return tcpClient -> tcpClient.doOnConnected(conn -> {
            var write = new WriteTimeoutHandler(writeTimeout, timeUnit);
            var read = new ReadTimeoutHandler(readTimeout, timeUnit);
            conn.removeHandler(OnChannelWriteIdle)
                    .addHandlerFirst(OnChannelWriteIdle, write);
            conn.removeHandler(OnChannelReadIdle)
                    .addHandlerFirst(OnChannelReadIdle, read);
        });
    }

    public static BiConsumer<HttpClientResponse, Connection> httpStatusChecker(int... statusCodes) {
        var set = new ArrayList<Integer>();
        for (int c : statusCodes) set.add(c);
        var statuses = Set.copyOf(set);
        return (response, connection) -> {
            var status = response.status();
            if (!statuses.contains(status.code())) {
                throw new HttpStatusException(status);
            }
        };
    }


    //

    public static final String GET = HttpMethod.GET.name();
    public static final String POST = HttpMethod.POST.name();
    public static final String PUT = HttpMethod.PUT.name();
    public static final String PATCH = HttpMethod.PATCH.name();
    public static final String DELETE = HttpMethod.DELETE.name();
    public static final String HEAD = HttpMethod.HEAD.name();
    public static final String TRACE = HttpMethod.TRACE.name();
    public static final String OPTIONS = HttpMethod.OPTIONS.name();

    public static final Map<String, HttpMethod> SUPPORT_METHODS;

    static {
        SUPPORT_METHODS = Map.of(
                GET, HttpMethod.GET,
                POST, HttpMethod.POST,
                PUT, HttpMethod.PUT,
                PATCH, HttpMethod.PATCH,
                DELETE, HttpMethod.DELETE,
                HEAD, HttpMethod.HEAD,
                TRACE, HttpMethod.TRACE,
                OPTIONS, HttpMethod.OPTIONS
        );
    }

    //

    public static <T> Mono<T> request(
            String method,
            String uri,
            ThrowoutConsumer<OutputStream, IOException> writer,
            Function<InputStream, T> translator) {
        assert null != method;
        assert null != uri && uri.length() > 0;
        assert null != writer;
        assert null != translator;

        var httpMethod = SUPPORT_METHODS.get(method);
        assert null != httpMethod;

        var body = ByteBufUtil.create(writer);
        return request(StatusCheckedClient.request(httpMethod).uri(uri).send(body), translator);
    }

    public static <T> Mono<T> get(
            String uri,
            Function<InputStream, T> translator) {
        assert null != uri && uri.length() > 0;

        return request(StatusCheckedClient.get().uri(uri), translator);
    }

    public static Mono<String> get(String uri) {
        assert null != uri && uri.length() > 0;

        return request(StatusCheckedClient.get().uri(uri));
    }

    public static Mono<String> post(
            String uri,
            String data) {
        assert null != uri && uri.length() > 0;
        assert null != data;

        var body = ByteBufUtil.create(data);
        return request(StatusCheckedClient.post().uri(uri).send(body));
    }

    public static Mono<String> post(
            String uri,
            byte[] bytes) {
        assert null != uri && uri.length() > 0;
        assert null != bytes;

        var body = ByteBufUtil.create(bytes);
        return request(StatusCheckedClient.post().uri(uri).send(body));
    }

    public static Mono<String> post(
            String uri,
            ThrowoutConsumer<OutputStream, IOException> writer) {
        assert null != uri && uri.length() > 0;
        assert null != writer;

        var body = ByteBufUtil.create(writer);
        return request(StatusCheckedClient.post().uri(uri).send(body));
    }

    public static <T> Mono<T> post(
            String uri,
            ThrowoutConsumer<OutputStream, IOException> writer,
            Function<InputStream, T> translator) {
        assert null != uri && uri.length() > 0;
        assert null != writer;

        var body = ByteBufUtil.create(writer);
        return request(StatusCheckedClient.post().uri(uri).send(body), translator);
    }

    public static Mono<String> post(String uri) {
        assert null != uri && uri.length() > 0;

        return request(StatusCheckedClient.post().uri(uri));
    }


    static <T> Mono<T> request(HttpClient.ResponseReceiver<?> receiver,
                               Function<InputStream, T> translator) {
        assert null != receiver;
        assert null != translator;

        return receiver.responseContent().aggregate().asInputStream().map(translator);
    }

    static Mono<String> request(HttpClient.ResponseReceiver<?> receiver) {
        assert null != receiver;

        return receiver.responseContent().aggregate().asString();
    }


    //


    private static final Charset charset = StandardCharsets.UTF_8;
    private static final String lineWrap = "\n";
    private static final String blank = " ";

    public static Mono<ByteBuf> makeTraceResponse(FullHttpRequest request) {
        return ByteBufUtil.createDirect((buf) -> {
            buf.writeCharSequence("TRACE", charset);
            buf.writeCharSequence(blank, charset);
            buf.writeCharSequence(request.uri(), charset);
            buf.writeCharSequence(blank, charset);
            buf.writeCharSequence("HTTP/1.1", charset);
            buf.writeCharSequence(lineWrap, charset);
            var headers = request.headers();
            headers.forEach((e) -> {
                var k = e.getKey();
                var v = e.getValue();
                if (null == v) return;
                buf.writeCharSequence(k, charset);
                buf.writeCharSequence(": ", charset);
                buf.writeCharSequence(v, charset);
                buf.writeCharSequence(lineWrap, charset);
            });
        });
    }

}
