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


/**
 * kafkaSource 的key和value的反序列化Schema
 */
@Slf4j
public class KafkaDeserializationSchema implements KafkaRecordDeserializationSchema<FaceProfile> {

    private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<FaceProfile> collector) {
        // key
        // {"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"mongo-compact.yisa_oe.face_profile.Key"},"payload":{"id":"1187"}}
        FaceProfile faceProfile = new FaceProfile();

        if (consumerRecord.value() == null) return;
        // 获取操作类型
        JSONObject payload = JSON.parseObject(consumerRecord.value()).getJSONObject("payload");
        faceProfile.setOperationType(payload.getString("op"));

        // 解析数据
        String afterStr = payload.getString("after");
        faceProfile.setFullDocument(JSON.parseObject(afterStr, FullDocument.class));

        if (faceProfile.getFullDocument() == null || faceProfile.getOperationType().equals("d")) {
            // 删除类型数据的处理
            FullDocument fullDocument = FullDocument.create();
            faceProfile.setFullDocument(fullDocument);
        }

        // 获取group
        faceProfile.getFullDocument().setGroup(JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id"));

        // 获取 associated_time 和 household_code
        JSONObject afterObject = JSONObject.parseObject(afterStr);
        // 特殊处理 long 类型数据
        parseLongData(faceProfile, afterObject);

        CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(afterObject, faceProfile);
        commonSchemaTransform.parseBytesData();
        collector.collect(faceProfile);
    }

    @Override
    public TypeInformation<FaceProfile> getProducedType() {
        return TypeInformation.of(FaceProfile.class);
    }

    /**
     * 解析 long 类型字段
     * @param faceProfile
     * @param after
     */
    private void parseLongData(FaceProfile faceProfile, JSONObject after) {
        if (after == null) {
            return;
        }

        // 获取数据在mongo操作时间
        if (after.containsKey("ttamp")) {
            faceProfile.getFullDocument().setInsertTime(after.getJSONObject("ttamp").getLong("$numberLong"));
        }

        if (after.containsKey("associated_time")) {
            faceProfile.getFullDocument().setAssociatedTime(after.getJSONObject("associated_time").getInteger("$numberLong"));
        }
    }
}
