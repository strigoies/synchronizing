package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.BaseData;
import com.yisa.model.FaceProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * kafkaSource 的key和value的反序列化Schema
 */
@Slf4j
public class KafkaDeserializationSchema implements KafkaRecordDeserializationSchema<BaseData> {

    private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<BaseData> collector) {
        // key
        // {"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"mongo-compact.yisa_oe.face_profile.Key"},"payload":{"id":"1187"}}

        if (consumerRecord.value() == null) return;
        // 获取操作类型
        JSONObject payload = JSON.parseObject(consumerRecord.value()).getJSONObject("payload");
        String op = payload.getString("op");

        // 解析数据
        String afterStr = payload.getString("after");
        FaceProfile faceProfile = JSON.parseObject(afterStr, FaceProfile.class);

        if (faceProfile == null || "d".equals(op)) {
            // 删除类型数据的处理
            faceProfile = FaceProfile.create();
            faceProfile.setIsDeleted((byte) 1);
        }

        // 获取group
        faceProfile.setGroup(JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id"));

        // 黑名单 数据不进行同步
        if (faceProfile.getBlackList() == 1) {
            faceProfile.setIsDeleted((byte) 1);
            log.info(String.format("检测到黑名单数据, group: %d", faceProfile.getGroup()));
        }
        // 获取 associated_time 和 household_code
        JSONObject afterObject = JSONObject.parseObject(afterStr);
        // 特殊处理 long 类型数据
        parseLongData(faceProfile, afterObject);

        CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(afterObject, faceProfile);
        commonSchemaTransform.parseBytesData();
        collector.collect(faceProfile);
    }

    @Override
    public TypeInformation<BaseData> getProducedType() {
        return TypeInformation.of(BaseData.class);
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
            faceProfile.setInsertTime(after.getJSONObject("ttamp").getLong("$numberLong"));
        }

        if (after.containsKey("associated_time")) {
            faceProfile.setAssociatedTime(after.getJSONObject("associated_time").getInteger("$numberLong"));
        }
    }
}
