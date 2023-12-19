package com.yisa.common;

import com.alibaba.fastjson2.JSON;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentDeleteEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.velocypack.VPackSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArangodbUtil {

    private static final Logger log = LoggerFactory.getLogger(ArangodbUtil.class);

    /**
     * 向arangodb插入文档或边缘, 通过对象的方式传入参数
     * @param collection
     * @param o
     */
    public static void insertDocumentFromObject(ArangoCollection collection, Object o) {
        try {
            DocumentCreateEntity<String> resp = collection.insertDocument(JSON.toJSONString(o));
            log.info("ArangoDB insert document success! id: {}, key: {}", resp.getId(), resp.getKey());
        } catch (ArangoDBException e) {
            log.error("ArangoDB insert document error! err code: {}, http code: {}, {}",
                    e.getErrorNum(), e.getResponseCode(), e.getErrorMessage());
            log.debug("debug info: {}", o.toString());
        }
    }

    /**
     * 更新arangodb文档, 通过base document方式
     * @param collection
     * @param bd
     */
    public static void updateDocumentFromBase(ArangoCollection collection, BaseDocument bd) {
        try {
            DocumentUpdateEntity<BaseDocument> resp = collection.updateDocument(bd.getKey(), bd);
            log.info("Arangodb update document success! id: {}, key: {}", resp.getId(), resp.getKey());
        } catch (ArangoDBException e) {
            log.error("ArangoDB update document error! err code: {}, http code: {} err: {}",
                    e.getErrorNum(), e.getResponseCode(), e.getErrorMessage());
            log.debug("debug info: {}", bd.getProperties());
        }
    }

    /**
     * 删除arangodb文档, 通过传入key
     * @param collection
     * @param key
     */
    public static void deleteDocument(ArangoCollection collection,String key){
        try {
            DocumentDeleteEntity<Void> voidDocumentDeleteEntity = collection.deleteDocument(key);
            log.info("Arangodb delete document success! id: {},key: {}",key,voidDocumentDeleteEntity);
        }catch (ArangoDBException e){
            log.error("Arangodb delete document error! err code: {},http code:{} err:{}",
                    e.getErrorNum(),e.getResponseCode(),e.getErrorMessage());
        }
    }

    /**
     * 执行arangodb aql语句
     * @param db
     * @param aql
     * @param vars
     * @return 返回字符串的list结果
     */
    public static List<String> queryArangoAsString(ArangoDatabase db, String aql, Map<String, Object> vars) {
        List<String> r = new ArrayList<>();
        try (ArangoCursor<String> cursor = db.query(aql, vars, null, String.class)) {
            while (cursor.hasNext())
                r.add(cursor.next());
        } catch (ArangoDBException e) {
            log.error("ArangoDB execute aql error! {}, aql: {}", e.getException(), aql);
        } catch (IOException e) {
            log.error("ArangoDB execute aql IO Exception! {}, aql: {}", e.getMessage(), aql);
        }
        return r;
    }

    /**
     * 执行arango aql语句
     * @param db
     * @param aql
     * @param vars
     * @return 返回VPackSlice类型元素的List
     */
    public static List<VPackSlice> queryArangoAsVPackSlice(ArangoDatabase db, String aql, Map<String, Object> vars) {
        List<VPackSlice> r = new ArrayList<>();
        try(ArangoCursor<VPackSlice> cursor = db.query(aql, vars, null, VPackSlice.class)) {
            while (cursor.hasNext())
                r.add(cursor.next());
        } catch (ArangoDBException e) {
            log.error("ArangoDB execute aql error! {}, aql: {}", e.getException(), aql);
        } catch (IOException e) {
            log.error("ArangoDB execute aql IO Exception! {}, aql: {}", e.getMessage(), aql);
        }
        return r;
    }

    /**
     * 根据集合名称和key获取文档的id
     * @param collectionName
     * @param key
     * @return
     */
    public static String getDocumentId(String collectionName, String key) {
        return String.format("%s/%s", collectionName, key);
    }

    /**
     * 按照文档key获取文档, 结果直接为string
     * @param collection
     * @param key
     * @return
     */
    public static String getDocumentByKey(ArangoCollection collection, String key) {
        try {
            return collection.getDocument(key, String.class);
        } catch (ArangoDBException e) {
            log.error("ArangoDB getDocument error! err code: {}, http code: {}, err: {}",
                    e.getErrorNum(), e.getResponseCode(), e.getErrorMessage());
            return null;
        }
    }
}
