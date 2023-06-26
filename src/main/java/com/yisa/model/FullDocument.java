package com.yisa.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

@Data
public class FullDocument {
    @JSONField(name = "_id", deserialize = false)
    private long group;

    // private TTamp ttamp;
    // @Data
    // public class TTamp {
    //     @JSONField(name = "$numberLong")
    //     private long numberLong;
    // }

    // private long g;
    // private int flag;
    // private long c_count;
    @JSONField(deserialize = false)
    private byte[] center;

    @JSONField(name = "new_id", deserialize = false)
    private byte[] newId;
    @JSONField(name = "g_count")
    private int groupCount;
    @JSONField(name = "personnel_name")
    private String personnelName;
    @JSONField(name = "personnel_id_number")
    private String personnelIdNumber;
    @JSONField(name = "personnel_photo_url")
    private String personnelPhotoUrl;
    @JSONField(name = "cosine_similarity", defaultValue = "0.0")
    private float cosineSimilarity;
    @JSONField(name = "special_type", defaultValue = "0")
    private short specialType;
    @JSONField(name = "associated_time", defaultValue = "0")
    private int associatedTime;
    @JSONField(deserialize = false)
    private int insertTime;

    // centers
    @JSONField(deserialize = false)
    private byte[][] centers;
    // source_ids
    @JSONField(name = "source_ids", deserialize = false)
    private short[] sourceIds;

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
    private short isDeleted;
}
