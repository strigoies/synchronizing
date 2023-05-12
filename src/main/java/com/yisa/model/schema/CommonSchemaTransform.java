package com.yisa.model.schema;

import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.FaceProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;

@Data
@Slf4j
@Builder
@AllArgsConstructor
public class CommonSchemaTransform {

    private static final String[] centers = new String[]{"center1", "center2", "center3", "center4", "center5"};
    private static final int sourceIdNum = 20;
    private static final String sourceIdPrefix = "source_id";

    private JSONObject fullDocumentData;
    private FaceProfile faceProfile;

    public void parseBytesData() {
        if (fullDocumentData == null || isBlack()) return;
        try {
            // 填充 center 和 centers
            setCenterAndCenters();
        } catch (Exception e) {
            log.error("解析 中心点失败, err:{}", e.getMessage());
        }

        try {
            // 填充 new_id
            setNewId();
        }catch (Exception e) {
            log.error("解析 new_id 失败, err:{}", e.getMessage());
        }
        try {
            // 填充 high_quality_id
            setHighQualityId();
        }catch (Exception e) {
            log.error("解析 高质量id失败, err:{}", e.getMessage());
        }

        try {
            // 填充 source_ids 列表
            setSourceIds();
        }catch (Exception e) {
            log.error("解析 source_ids 列表失败, err:{}", e.getMessage());
        }

    }
    /**
     * 判断该组是否为黑名单数据
     * @param
     * @return
     */
    private boolean isBlack() {
        // 判断是否为黑名单
        if (fullDocumentData.containsKey("black_list")){
            return true;
        }
        return false;
    }

    private void setCenterAndCenters() {
        // 填充 center 和 centers
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
    }

    private void setNewId() {
        String newIdStr = fullDocumentData.getString("new_id");
        faceProfile.getFullDocument().setNewId(convertUUIDToBytes(newIdStr));
    }
    private void setHighQualityId() {
        String highQualityIdStr = fullDocumentData.getString("high_quality_id");
        faceProfile.getFullDocument().setHighQualityId(convertUUIDToBytes(highQualityIdStr));
    }

    private void setSourceIds() {
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
