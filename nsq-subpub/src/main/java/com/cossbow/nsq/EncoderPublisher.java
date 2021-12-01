package com.cossbow.nsq;

import com.cossbow.nsq.util.Encoder;

import java.util.concurrent.CompletableFuture;

/**
 * 绑定编码器{@link Encoder}的{@link NsqPublisher}，方便使用
 */
public interface EncoderPublisher {

    CompletableFuture<Void> publish(String topic, int defer, Object value);

    default CompletableFuture<Void> publish(String topic, Object value) {
        return publish(topic, 0, value);
    }


    //
    //
    //

    static EncoderPublisher wrapEncoder(NsqPublisher publisher, Encoder encoder) {
        return (topic, defer, value) -> publisher.publish(topic, defer, value, encoder);
    }

}
