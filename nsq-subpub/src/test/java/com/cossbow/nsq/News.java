package com.cossbow.nsq;


import lombok.Data;

import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;

@Data
public class News implements Serializable {
    private static final long serialVersionUID = 1052075161334157354L;

    private int id;
    private String title;
    private String content;
    private Instant createdTime;
    private OffsetDateTime publishedTime;
}
