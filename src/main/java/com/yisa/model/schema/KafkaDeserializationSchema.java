package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.FaceProfile;
import com.yisa.model.FullDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * kafkaSource 的key和value的反序列化Schema
 */
@Slf4j
public class KafkaDeserializationSchema implements KafkaRecordDeserializationSchema<FaceProfile> {

    private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<FaceProfile> collector) throws IOException {
        FaceProfile faceProfile = new FaceProfile();
        // 获取group
        faceProfile.setGroup(JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id"));

        if (consumerRecord.value() == null) return;
        // 获取操作类型
        JSONObject payload = JSON.parseObject(consumerRecord.value()).getJSONObject("payload");
        faceProfile.setOperationType(payload.getString("op"));

        String fullDocumentStr = payload.getString("after");
        faceProfile.setFullDocument(JSON.parseObject(fullDocumentStr, FullDocument.class));

        if (fullDocumentStr != null || faceProfile.getOperationType().equals("d")) {
            JSONObject fullDocumentData = JSONObject.parseObject(fullDocumentStr);
            CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(fullDocumentData, faceProfile);
            commonSchemaTransform.parseBytesData();
            collector.collect(faceProfile);
        }
    }

    @Override
    public TypeInformation<FaceProfile> getProducedType() {
        return TypeInformation.of(FaceProfile.class);
    }
}
