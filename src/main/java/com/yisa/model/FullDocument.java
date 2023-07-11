package com.yisa.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

@Data
public class FullDocument {
    @JSONField(name = "_id", deserialize = false)
    private long group;
    @JSONField(deserialize = false)
    private byte[] center;

    @JSONField(name = "new_id", deserialize = false)
    private byte[] newId;
    @JSONField(name = "g_count", defaultValue = "0")
    private int groupCount;
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
    private byte[][] centers;
    // source_types
    @JSONField(deserialize = false)
    private short[][] sourceTypes;

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
    private byte[] highQualityId;

    /* 是否是黑名单（空则不为黑名单）反序列化时判断是否有此属性 */
    @JSONField(name = "black_list", defaultValue = "0")
    private byte blackList;

    @JSONField(name = "is_deleted", deserialize = false)
    private byte isDeleted;

    private FullDocument() {
        this.group = 0;
        this.center = new byte[0];
        this.newId = new byte[0];
        this.groupCount = 0;
        this.personnelName = "";
        this.personnelIdNumber = "";
        this.personnelPhotoUrl = "";
        this.cosineSimilarity = 0;
        this.associatedTime = 0;
        this.insertTime = 0;
        this.centers = new byte[0][];
        this.sourceTypes = new short[0][];
        this.householdCode = 0;
        this.householdAddress = "";
        this.birthday = 0;
        this.gender = 0;
        this.highQualityId = new byte[0];
        this.blackList = 0;
        this.isDeleted = 0;
    }

    public static FullDocument create() {
        return new FullDocument();
    }
}
