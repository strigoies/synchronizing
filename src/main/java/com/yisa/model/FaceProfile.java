package com.yisa.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@Builder
@AllArgsConstructor
public class FaceProfile {
    /**
     * group(与内部类FullDocument的group一致)
     */
    @JSONField(serialize = false)
    private long group;

    /**
     * operationType : insert
     */
    private String operationType;

    /**
     * 整条数据
     */
    private FullDocument fullDocument;

    public FaceProfile() {
        fullDocument = new FullDocument();
    }

    /**
     * 设置删除的数据
     */
    public void setDeleteTag(long group) {
        fullDocument = new FullDocument();
        fullDocument.setIsDeleted((byte) -1);
        fullDocument.setGroup(group);
    }
}


