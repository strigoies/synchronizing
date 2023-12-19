package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.ArangoDBMark;
import com.yisa.model.FaceProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Int;


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

        if (consumerRecord.value() == null) return;
        // 获取操作类型
        JSONObject payload = JSON.parseObject(consumerRecord.value()).getJSONObject("payload");
        String op = payload.getString("op");

        // 特殊处理删除数据和黑名单
        if ("d".equals(op) || payload.getJSONObject("after").getIntValue("blackList") == 1) {
            handleDeletedData(consumerRecord.key(), collector);
            return;
        }

        // 写入雷霆的数据
        // 解析数据
        Long group = JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id");
        FaceProfile faceProfile = JSON.parseObject(payload.getString("after"), FaceProfile.class);
        Integer[] existLabels = JSON.parseObject(payload.getJSONObject("before").getString("labels"), Integer[].class);
        Integer[] afterLabels = faceProfile.getLabels();

        // 没有标签的数据直接跳过
        if (existLabels == null && afterLabels == null) return;

        // 存储变化状态
        ArangoDBMark arangoDBMark = new ArangoDBMark();
        arangoDBMark.setOp(op);
        arangoDBMark.setGroup(group);
        arangoDBMark.setPersonnelIdNumber(faceProfile.getPersonnelIdNumber());
        arangoDBMark.setPersonnelIdType(faceProfile.getPersonnelIdType());
        arangoDBMark.setAfterLabels(afterLabels);
        arangoDBMark.setBeforeLabels(existLabels);

        // 处理公共数据转换
        handleCommonSchemaTransform(payload.getJSONObject("after"), faceProfile, arangoDBMark,collector);

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
            faceProfile.setInsertTime(after.getJSONObject("ttamp").getLong("$numberLong"));
        }

        if (after.containsKey("associated_time")) {
            faceProfile.setAssociatedTime(after.getJSONObject("associated_time").getInteger("$numberLong"));
        }
    }

    private void handleDeletedData(byte[] key, Collector<FaceProfile> collector) {
        FaceProfile faceProfile = FaceProfile.create();
        faceProfile.setIsDeleted((byte) 1);
        ArangoDBMark arangoDBMark = new ArangoDBMark();
        arangoDBMark.setOp("d");
        arangoDBMark.setGroup(JSON.parseObject(key).getJSONObject("payload").getLong("id"));

        // 黑名单 数据不进行同步
        if (faceProfile.getBlackList() == 1) {
            log.info(String.format("检测到黑名单数据, group: %d", faceProfile.getGroup()));
        }

        faceProfile.setGroup(arangoDBMark.getGroup());
        collector.collect(faceProfile);
        collector.collect(arangoDBMark);
    }

    private void handleCreateData(FaceProfile faceProfile, Long group, ArangoDBMark arangoDBMark,Integer[] existLabels) {
        arangoDBMark.setOp("c");
        arangoDBMark.setGroup(group);
        arangoDBMark.setPersonnelIdNumber(faceProfile.getPersonnelIdNumber());
        arangoDBMark.setPersonnelIdType(faceProfile.getPersonnelIdType());
        arangoDBMark.setLabels(faceProfile.getLabels());
        arangoDBMark.setBeforeLabels(existLabels);
    }

    private void handleUpdateData(JSONObject payload, Long group, ArangoDBMark arangoDBMark, Integer[] existLabels) {
        JSONObject updatedFields = payload.getJSONObject("updateDescription").getJSONObject("updatedFields");
        arangoDBMark.setOp("u");
        arangoDBMark.setGroup(group);
        arangoDBMark.setPersonnelIdNumber(updatedFields.getString("personnel_id_number"));
        arangoDBMark.setPersonnelIdType(updatedFields.getString("personnel_id_type"));
        Integer[] labels = updatedFields.getJSONArray("labels").toArray(Integer.class);
        arangoDBMark.setLabels(labels);
        arangoDBMark.setBeforeLabels(existLabels);
    }

    private void handleCommonSchemaTransform(JSONObject afterObject, FaceProfile faceProfile, ArangoDBMark arangoDBMark,Collector<FaceProfile> collector) {
        // 获取 associated_time 和 household_code
        parseLongData(faceProfile, afterObject);

        CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(afterObject, faceProfile);
        commonSchemaTransform.parseBytesData();
        collector.collect(faceProfile);
        collector.collect(arangoDBMark);
    }
}
