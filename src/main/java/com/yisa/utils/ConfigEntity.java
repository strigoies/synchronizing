package com.yisa.utils;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 设置类实体
 */
@Data
public class ConfigEntity implements Serializable {

    /**
     * kafka 配置
     */
    private Kafka kafka;
    /**
     * 雷霆配置
     */
    private LightningDB lightningDB;

    /**
     * 程序其他配置
     */
    private Parameter parameter;
    /* ----------------- 内部设置类 ---------------------- */

    @Data
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
    public static class LightningDB implements Serializable {
        private List<List> hosts;
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
    public static class Parameter implements Serializable {
        @JSONField(deserialize = false)
        private String jobName;
        private boolean disableOperatorChain;
        private long delayBetweenAttempts;
        private long checkPoint;
    }
}