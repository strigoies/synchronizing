package com.yisa.model;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;

@Slf4j
public class FaceProfileSchema implements DebeziumDeserializationSchema<FaceProfile> {

    private static final long serialVersionUID = -3168848963265670603L;
    private static final String[] centers = new String[]{"center1", "center2", "center3", "center4", "center5"};

    private static final int sourceIdNum = 20;
    private static final String sourceIdPrefix = "source_id";

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<FaceProfile> collector) throws Exception {
        Struct value = (Struct) sourceRecord.value();
        FaceProfile faceProfile = new FaceProfile();
        // 获取 documentKey 对应 雷霆的字段 group
        // documentKey={"_id": 68279}
        String idStruct = value.getString("documentKey");
        JSONObject idJson = JSON.parseObject(idStruct);
        Long group = idJson.getLong("_id");
        faceProfile.setGroup(group);

        // 获取操作类型
        String operationType = value.getString("operationType");
        faceProfile.setOperationType(operationType);

        // 获取数据
        String fullDocumentStr = value.getString("fullDocument");
        faceProfile.setFullDocument(JSON.parseObject(fullDocumentStr, FullDocument.class));

        if (fullDocumentStr!=null) {
            // 填充 center 和 centers
            JSONObject fullDocumentData = JSONObject.parseObject(fullDocumentStr);
            // 判断是否为黑名单
            if (fullDocumentData.containsKey("black_list")){
                return;
            }
            ArrayList<byte[]> bytesList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                if (!fullDocumentData.containsKey(centers[i])) {
                    continue;
                }
                byte[] centerBytes = fullDocumentData.getJSONObject(centers[i]).getBytes("$binary");
                if (centerBytes == null || centerBytes.length == 0) {
                    continue;
                }
                if (i == 0) {
                    faceProfile.getFullDocument().setCenter(centerBytes);
                }
                bytesList.add(centerBytes);
            }
            faceProfile.getFullDocument().setCenters(bytesList.toArray(new byte[bytesList.size()][]));

            // 填充 high_quality_id
            String highQualityIdStr = fullDocumentData.getString("high_quality_id");
            faceProfile.getFullDocument().setHighQualityId(convertUUIDToBytes(highQualityIdStr));

            // 填充 source_ids
            ArrayList<Short> sourceIdList = new ArrayList<>();
            for (int i = 1; i <= sourceIdNum; i++) {
                if (!fullDocumentData.containsKey(sourceIdPrefix+i)) {
                    continue;
                }
                Short sourceIdNum = fullDocumentData.getShort(sourceIdPrefix + i);
                sourceIdList.add(sourceIdNum);
            }
            short[] sourceIdsShort = new short[sourceIdList.size()];
            for (int i = 0; i < sourceIdList.size(); i++) {
                sourceIdsShort[i] = sourceIdList.get(i);
            }
            faceProfile.getFullDocument().setSourceIds(sourceIdsShort);
        }

        collector.collect(faceProfile);
    }

    @Override
    public TypeInformation<FaceProfile> getProducedType() {
        return TypeInformation.of(FaceProfile.class);
    }

    private static byte[] convertUUIDToBytes(String uuidStr) {
        try {
            UUID uuid = UUID.fromString(uuidStr);
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        } catch (Exception e) {
            return new byte[16];
        }
    }
}
