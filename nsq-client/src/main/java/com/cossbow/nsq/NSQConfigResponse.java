package com.cossbow.nsq;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
class NSQConfigResponse {
    @JsonProperty("max_rdy_count")
    private int maxRdyCount;
    @JsonProperty("max_msg_timeout")
    private int maxMsgTimeout;
    @JsonProperty("msg_timeout")
    private int msgTimeout;
    @JsonProperty("tls_v1")
    private boolean tlsV1;
    private boolean deflate;
    @JsonProperty("deflate_level")
    private int deflateLevel;
    @JsonProperty("max_deflate_level")
    private int maxDeflateLevel;
    private boolean snappy;
    @JsonProperty("sample_rate")
    private int sampleRate;
    @JsonProperty("auth_required")
    private boolean authRequired;
    @JsonProperty("output_buffer_size")
    private int outputBufferSize;
    @JsonProperty("output_buffer_timeout")
    private int outputBufferTimeout;
}
