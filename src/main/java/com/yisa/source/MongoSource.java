package com.yisa.source;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.yisa.model.FaceProfile;
import com.yisa.model.schema.MongoDeserializationSchema;
import com.yisa.utils.ConfigEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoSource {
    public static DataStreamSource<FaceProfile> getMongoStream(StreamExecutionEnvironment env, ConfigEntity.MongoDB mongodb) {
        MongoDBSource<FaceProfile> mongoSource =
                MongoDBSource.<FaceProfile>builder()
                        .hosts(mongodb.getHosts())
                        .databaseList(mongodb.getDatabase())
                        .collectionList(mongodb.getDatabase() + "." + mongodb.getCollection())
                        .username(mongodb.getUsername())
                        .password(mongodb.getPassword())
                        .deserializer(new MongoDeserializationSchema())
                        .build();
        return env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBSource");
    }
}
