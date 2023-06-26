package com.yisa.utils;

import com.alibaba.fastjson2.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

import java.io.Serializable;

/**
 * 设置类实体
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigEntity implements Serializable {

    /**
     * kafka 配置
     */
    private Kafka kafka;
    /**
     * 雷霆配置
     */
    private LightningDB lightningdb;

    /**
     * 程序其他配置
     */
    private Parameter parameter;
    /* ----------------- 内部设置类 ---------------------- */

    @Data
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Kafka implements Serializable {
        private String hosts;
        private String faceProfileTopic;
        private String faceProfilePlateTopic;
        @JSONField(deserialize = false)
        private String activeTopic;
        private String groupId;
        private String offset;
    }

    @Data
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class LightningDB implements Serializable {
        private String hosts;
        private String database;
        private String username;
        private String password;
        private String faceProfileDistributedTable;
        private String faceProfilePlateDistributedTable;
        @JSONField(deserialize = false)
        private String activeTable;
        private int batchSize;
        private int flushInterval;
        private int maxRetries;
    }
    @Data
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Parameter implements Serializable {
        @JSONField(deserialize = false)
        private String jobName;
        private boolean disableOperatorChain;
        private long delayBetweenAttempts;
        private long checkPoint;
    }
}