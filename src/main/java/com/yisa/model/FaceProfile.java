package com.yisa.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FaceProfile extends BaseData {
    @JSONField(name = "_id", deserialize = false)
    private long group;
    @JSONField(deserialize = false)
    private byte[] center = new byte[0];

    @JSONField(name = "new_id", deserialize = false)
    private byte[] newId = new byte[0];
    @JSONField(name = "grout_count", defaultValue = "0")
    private int groupCount;
    @JSONField(name = "face_count", defaultValue = "0")
    private int faceCount;
    @JSONField(name = "person_count", defaultValue = "0")
    private int personCount;
    @JSONField(name = "age_id",defaultValue = "0")
    private short ageId;

    @JSONField(name = "personnel_name")
    private String personnelName;
    @JSONField(name = "personnel_id_number")
    private String personnelIdNumber;
    @JSONField(name = "personnel_photo_url")
    private String personnelPhotoUrl;
    @JSONField(name = "cosine_similarity", defaultValue = "0.0")
    private float cosineSimilarity;
    @JSONField(name = "associated_time", defaultValue = "0")
    private int associatedTime;
    @JSONField(deserialize = false)
    private long insertTime;

    // centers
    @JSONField(deserialize = false)
    private byte[][] centers = new byte[0][];
    // source_types
    @JSONField(deserialize = false)
    private short[][] sourceTypes = new short[0][];

    @JSONField(name = "household_code", defaultValue = "0")
    private int householdCode;
    @JSONField(name = "household_address")
    private String householdAddress;

    @JSONField(defaultValue = "0")
    private int birthday;
    @JSONField(defaultValue = "0")
    private short gender;
    // private int g_quality_int;
    @JSONField(name = "high_quality_id", deserialize = false)
    private byte[] highQualityId = new byte[0];

    /* 是否是黑名单（空则不为黑名单）反序列化时判断是否有此属性 */
    @JSONField(name = "black_list", defaultValue = "0")
    private byte blackList;

    /**
     * fusion3.0 标签
     */
    @JSONField(deserialize = false)
    private Integer[] labels = new Integer[0];

    @JSONField(name = "is_deleted", deserialize = false)
    private byte isDeleted;

    public static FaceProfile create() {
        return new FaceProfile();
    }
}
