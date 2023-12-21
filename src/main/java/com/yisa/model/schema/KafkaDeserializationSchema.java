package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.ArangoDBMark;
import com.yisa.model.FaceProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.Objects;


/**
 * kafkaSource 的key和value的反序列化Schema
 */
@Slf4j
public class KafkaDeserializationSchema implements KafkaRecordDeserializationSchema<ArangoDBMark> {

    private static final String KEY_PAYLOAD = "payload";
    private static final String KEY_BEFORE = "before";
    private static final String KEY_AFTER = "after";
    private static final String KEY_OP = "op";
    private static final String KEY_UPDATE_DESCRIPTION = "updateDescription";
     private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<ArangoDBMark> collector) {
        // key
        // {"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"}],"optional":false,"name":"mongo-compact.yisa_oe.face_profile.Key"},"payload":{"id":"1187"}}

        if (consumerRecord.value() == null) return;
        // 获取操作类型
        JSONObject payload = JSON.parseObject(consumerRecord.value()).getJSONObject(KEY_PAYLOAD);
        String op = payload.getString(KEY_OP);

        // 特殊处理删除数据和黑名单
        if ("d".equals(op) || payload.getJSONObject(KEY_AFTER).getIntValue("blackList") == 1) {
            ArangoDBMark arangoDBMark = new ArangoDBMark();
            arangoDBMark.setOp("d");
            arangoDBMark.setGroup(JSON.parseObject(consumerRecord.key()).getJSONObject("payload").getLong("id"));
            Integer[] beforeLabels = JSON.parseObject(payload.getJSONObject(KEY_BEFORE).getString("labels"), Integer[].class);
            if (beforeLabels != null) arangoDBMark.setBeforeLabels(beforeLabels);
            else arangoDBMark.setBeforeLabels(new Integer[0]);

            FaceProfile faceProfile = FaceProfile.create();
            faceProfile.setIsDeleted((byte) 1);
            faceProfile.setGroup(arangoDBMark.getGroup());
            // 黑名单 数据不进行同步
            if (faceProfile.getBlackList() == 1) {
                log.info(String.format("检测到黑名单数据, group: %d", faceProfile.getGroup()));
            }
            collector.collect(arangoDBMark);
            return;
        }

        // 写入雷霆的数据
        // 解析数据
        Long group = JSON.parseObject(consumerRecord.key()).getJSONObject(KEY_PAYLOAD).getLong("id");
        FaceProfile faceProfile = JSON.parseObject(payload.getString(KEY_AFTER), FaceProfile.class);

        // 获取group
        faceProfile.setGroup(group);
        handleCommonSchemaTransform(payload.getJSONObject(KEY_AFTER), faceProfile);

        // 写入ArangoDB的数据
        Integer[] beforeLabels = new Integer[0];
        ArangoDBMark arangoDBMark = new ArangoDBMark();
        arangoDBMark.setBeforeLabels(beforeLabels);
        arangoDBMark.setAfterLabels(faceProfile.getLabels());
        arangoDBMark.setOp(op);
        arangoDBMark.setGroup(group);
        arangoDBMark.setFaceProfile(faceProfile);
        if ("u".equals(op)){
            FaceProfile faceProfileBefore = JSON.parseObject(payload.getString(KEY_AFTER), FaceProfile.class);
            JSONObject beforeJsonObject = payload.getJSONObject(KEY_BEFORE);
            String beforePersonnelIdNumber = JSON.parseObject(beforeJsonObject.getString("personnel_id_number"),String.class);
            String beforePersonnelIdType = JSON.parseObject(beforeJsonObject.getString("personnel_id_type"),String.class);
            beforeLabels = JSON.parseObject(beforeJsonObject.getString("labels"), Integer[].class);
            if (beforeLabels != null) arangoDBMark.setBeforeLabels(beforeLabels);

            // 获取身份证信息更新状态
            if (payload.containsKey(KEY_UPDATE_DESCRIPTION) && payload.getJSONObject(KEY_UPDATE_DESCRIPTION) != null){
                // 处理存在字段更新
                JSONObject udpateJsonObject = payload.getJSONObject(KEY_UPDATE_DESCRIPTION).getJSONObject("updatedFields");
                JSONArray removeJsonObject = payload.getJSONObject(KEY_UPDATE_DESCRIPTION).getJSONArray("removedFields");
                // 更新字段
                if (udpateJsonObject != null && ((udpateJsonObject.containsKey("personnel_id_number") || udpateJsonObject.containsKey("personnel_id_type")))){
                    String personnelIdNumber = JSON.parseObject(udpateJsonObject.getString("personnel_id_number"),String.class);
                    String personnelIdType = JSONObject.parseObject(udpateJsonObject.getString("personnel_id_type"),String.class);
                    arangoDBMark.setBeforePersonnelIdNumber(beforePersonnelIdNumber);
                    arangoDBMark.setBeforePersonnelIdType(beforePersonnelIdType);
                    if (personnelIdNumber != null){
                        arangoDBMark.setAfterPersonnelIdNumber(personnelIdNumber);
                    }else {
                        // 不更新证件号，只更新证件类型(罕见情况)
                        arangoDBMark.setAfterPersonnelIdNumber(faceProfile.getPersonnelIdNumber());
                    }
                    if (personnelIdType != null){
                        arangoDBMark.setAfterPersonnelIdType(personnelIdType);
                    }else {
                        // 不更新证件类型，只更新证件号码(常见)
                        arangoDBMark.setAfterPersonnelIdType(faceProfile.getPersonnelIdType());
                    }
                }
                // 删除字段
                if(removeJsonObject != null && (removeJsonObject.contains("personnel_id_number") || removeJsonObject.contains("personnel_id_type"))){
                    arangoDBMark.setBeforePersonnelIdNumber(beforePersonnelIdNumber);
                    arangoDBMark.setBeforePersonnelIdType(beforePersonnelIdType);
                    String personnelIdNumber = JSON.parseObject(beforeJsonObject.getString("personnel_id_number"),String.class);
                    String personnelIdType = JSONObject.parseObject(beforeJsonObject.getString("personnel_id_type"),String.class);
                    if (personnelIdNumber != null && personnelIdType == null){
                        //只删除了证件类型，没删证件号码
                        log.error("需同时删除personnel_id_number");
                    }else if (personnelIdType != null && personnelIdNumber == null){
                        log.error("需同时删除personnel_id_type");

                    }else {
                        arangoDBMark.setAfterPersonnelIdNumber("");
                        arangoDBMark.setAfterPersonnelIdType("");
                    }
                }
            }else{
                // 处理新增字段的情况
                FaceProfile faceProfileDiffer = compareBeforeAndAfter(faceProfileBefore, faceProfile);
                arangoDBMark.setAfterPersonnelIdNumber(faceProfileDiffer.getPersonnelIdNumber());
                arangoDBMark.setAfterPersonnelIdType(faceProfileDiffer.getPersonnelIdType());
                arangoDBMark.setAfterLabels(faceProfileDiffer.getLabels());
            }

        }else if("c".equals(op)){
            arangoDBMark.setAfterPersonnelIdNumber(faceProfile.getPersonnelIdNumber());
            arangoDBMark.setAfterPersonnelIdType(faceProfile.getPersonnelIdType());
        }
        collector.collect(arangoDBMark);
    }

    @Override
    public TypeInformation<ArangoDBMark> getProducedType() {
        return TypeInformation.of(ArangoDBMark.class);
    }

    /**
     * 解析 long 类型字段
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

    private void handleCommonSchemaTransform(JSONObject afterObject, FaceProfile faceProfile) {
        // 获取 associated_time 和 household_code
        parseLongData(faceProfile, afterObject);
        CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(afterObject, faceProfile);
        commonSchemaTransform.parseBytesData();
    }

    public static FaceProfile compareBeforeAndAfter(FaceProfile before, FaceProfile after) {
        FaceProfile difference = new FaceProfile();

        if (!Objects.equals(before.getPersonnelIdType(), after.getPersonnelIdType())) {
            difference.setPersonnelIdType(after.getPersonnelIdType());
            difference.setPersonnelIdNumber(after.getPersonnelIdNumber());
            difference.setLabels(after.getLabels());
        }
        if (!Objects.equals(before.getPersonnelIdNumber(), after.getPersonnelIdNumber())) {
            difference.setPersonnelIdType(after.getPersonnelIdType());
            difference.setPersonnelIdNumber(after.getPersonnelIdNumber());
            difference.setLabels(after.getLabels());
        }
        if (!Arrays.equals(before.getLabels(), after.getLabels())) {
            difference.setPersonnelIdType(after.getPersonnelIdType());
            difference.setPersonnelIdNumber(after.getPersonnelIdNumber());
            difference.setLabels(after.getLabels());
        }

        return difference;
    }
}
