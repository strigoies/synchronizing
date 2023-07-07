package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.yisa.model.FaceProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
@Slf4j
@Builder
@AllArgsConstructor
public class CommonSchemaTransform {

    private static final String[] CENTERS = new String[]{"center1", "center2", "center3", "center4", "center5"};
    private static final String DEVICE_TYPE_NAME = "device_type";
    private static final String OBJECT_TYPE_NAME = "object_type";
    private static final String SOURCE_TYPES_NAME = "source_types";

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
            // 填充 source_types 列表, 对应雷霆表字段 device_object_types
            setSourceTypes();
        }catch (Exception e) {
            log.error("解析 source_types 列表失败, err:{}", e.getMessage());
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
            if (!fullDocumentData.containsKey(CENTERS[i])) {
                continue;
            }
            byte[] centerBytes = fullDocumentData.getJSONObject(CENTERS[i]).getBytes("$binary");
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

    private void setSourceTypes() {
        JSONArray sourceTypes = fullDocumentData.getJSONArray(SOURCE_TYPES_NAME);
        short[][] array = new short[sourceTypes.size()][];
        for (int i = 0; i < sourceTypes.size(); i++) {
            // {"device_type":2,"object_type":6}
            JSONObject jsonObject = JSONObject.parseObject(sourceTypes.get(i).toString());
            array[i] = new short[]{jsonObject.getShort(DEVICE_TYPE_NAME), jsonObject.getShort(OBJECT_TYPE_NAME)};
        }
        faceProfile.getFullDocument().setSourceTypes(array);
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
