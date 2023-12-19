package com.yisa.utils;

public class DataTransform {
    /**
     * 根据arangodb document id获取collection和key方法2, 采用分隔数组方式, 性能比较高
     *
     * @param docId
     * @return
     */
    public static String[] getDocIdToCollectionAndKey2(String docId) {
        return docId.split("/");
    }
}
