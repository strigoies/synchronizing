package com.yisa.source;

import com.yisa.model.ArangoDBMark;
import com.yisa.model.FaceProfile;
import com.yisa.model.schema.KafkaDeserializationSchema;
import com.yisa.utils.ConfigEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class KafkaSource {
    public static SingleOutputStreamOperator<ArangoDBMark> getKafkaStream(StreamExecutionEnvironment env, ConfigEntity.Kafka kafka) {
        String offsetReset = kafka.getOffset();
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.latest();
        if (offsetReset.equals("earliest")) {
            offsetsInitializer = OffsetsInitializer.earliest();
        } else if (offsetReset.equals("latest")) {
            offsetsInitializer = OffsetsInitializer.latest();
        } else {
            log.error("kafkaSource 设置开始消费的偏移量错误");
        }

        org.apache.flink.connector.kafka.source.KafkaSource<ArangoDBMark> kafkaSource = org.apache.flink.connector.kafka.source.KafkaSource.<ArangoDBMark>builder()
                .setBootstrapServers(kafka.getHosts())
                .setTopics(kafka.getActiveTopic())
                .setGroupId(kafka.getGroupId())
                .setDeserializer(new KafkaDeserializationSchema())
                .setStartingOffsets(offsetsInitializer)
                .setProperty("enable.auto.commit", "true")
                .build();

        WatermarkStrategy<ArangoDBMark> strategy = WatermarkStrategy.noWatermarks();
        return env.fromSource(kafkaSource, strategy, "kafkaSource").returns(TypeInformation.of(ArangoDBMark.class));
    }
}
