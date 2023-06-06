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
        String fullDocumentStr = payload.getString("after");
        faceProfile.setFullDocument(JSON.parseObject(fullDocumentStr, FullDocument.class));
        if (faceProfile.getFullDocument() == null && faceProfile.getOperationType().equals("d")) {
            FullDocument fullDocument = new FullDocument();
            faceProfile.setFullDocument(fullDocument);
        }
        // 获取group
        faceProfile.getFullDocument().setGroup(JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id"));

        // 获取 associated_time 和 household_code
        JSONObject after = payload.getJSONObject("after");
        if (after != null) {
            if (after.containsKey("associated_time")) {
                faceProfile.getFullDocument().setAssociatedTime(after.getJSONObject("associated_time").getInteger("$numberLong"));
            }

            if (after.containsKey("household_code")) {
                faceProfile.getFullDocument().setHouseholdCode(after.getJSONObject("household_code").getInteger("$numberLong"));
            }
        }

        // 获取数据在mongo操作时间
        // "source":{"version":"2.0.0.Final","connector":"mongodb","name":"mongo-shard","ts_ms":1683861694000,"snapshot":"false","db":"yisa_oe","sequence":null,"rs":"shard01","collection":"face_group","ord":1,"lsid":null,"txnNumber":null}
        faceProfile.getFullDocument().setInsertTime((int)(payload.getJSONObject("source").getLong("ts_ms")/1000));

        JSONObject fullDocumentData = JSONObject.parseObject(fullDocumentStr);
        CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(fullDocumentData, faceProfile);
        commonSchemaTransform.parseBytesData();
        collector.collect(faceProfile);
    }

    @Override
    public TypeInformation<FaceProfile> getProducedType() {
        return TypeInformation.of(FaceProfile.class);
    }
}
