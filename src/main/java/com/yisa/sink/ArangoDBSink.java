package com.yisa.sink;

import com.arangodb.*;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.util.MapBuilder;
import com.yisa.model.ArangoDBMark;
import com.yisa.model.FaceProfile;
import com.yisa.model.Relation;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.DataTransform;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.*;

import static com.yisa.common.ArangodbUtil.*;

@Slf4j
public class ArangoDBSink extends RichSinkFunction<FaceProfile> {

    private static final int LABEL_RELATION_TYPE = 101;

    private static final String  LABEL_RELATION_NAME = "人和标签的关系";
    private ConfigEntity.ArangoDB arangoConfig;
    private ArangoDB arangoDB;
    private ArangoDatabase arangoDatabase;
    private ArangoCollection relationEdgeCollection;

    private ArangoCollection activeTable;

    private  ArangoCollection personnelInfoTable;
    private  ArangoCollection labelTable;

    public ArangoDBSink() {}

    public ArangoDBSink(ConfigEntity.ArangoDB config) {
        this.arangoConfig = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("初始化ArangoDB实例. {}", arangoConfig.toString());
        List<List> arangoDBHostPorts = arangoConfig.getHosts();
        int index = new Random().nextInt(arangoDBHostPorts.size());
        arangoDB = new ArangoDB.Builder()
                .host((String) arangoDBHostPorts.get(index).get(0), (Integer) arangoDBHostPorts.get(index).get(1))
                .user(arangoConfig.getUsername()).password(arangoConfig.getPassword())
                .build();
        arangoDatabase = arangoDB.db(arangoConfig.getDatabase());
        if(!arangoDatabase.exists()) {
            log.warn("ArangoDB数据库: {} 不存在, 自动创建.", arangoConfig.getDatabase());
            arangoDatabase.create();
        }
        // 关系表
        relationEdgeCollection = arangoDatabase.collection(arangoConfig.getRelationEdgeTable());
        if(!relationEdgeCollection.exists()) {
            log.warn("Arangodb relation edge: {} 不存在, 自动创建. ", arangoConfig.getRelationEdgeTable());
            relationEdgeCollection.create(new CollectionCreateOptions().type(CollectionType.EDGES));
        }
        // 人员信息表
        personnelInfoTable = arangoDatabase.collection(arangoConfig.getPersonnelInfoTable());
        if(!personnelInfoTable.exists()) {
            log.warn("Arangodb personnel_info document: {} 不存在, 自动创建. ", arangoConfig.getPersonnelInfoTable());
            personnelInfoTable.create(new CollectionCreateOptions().type(CollectionType.DOCUMENT));
        }
        // 人脸聚类表/驾乘人脸聚类表
        activeTable = arangoDatabase.collection(arangoConfig.getActiveTable());
        if(!activeTable.exists()) {
            log.warn("Arangodb document: {} 不存在, 自动创建. ", arangoConfig.getActiveTable());
            activeTable.create(new CollectionCreateOptions().type(CollectionType.DOCUMENT));
        }
        // 标签表
        labelTable = arangoDatabase.collection(arangoConfig.getLabelTable());
        if(!labelTable.exists()) {
            log.warn("Arangodb document: {} 不存在, 自动创建. ", arangoConfig.getLabelTable());
            labelTable.create(new CollectionCreateOptions().type(CollectionType.DOCUMENT));
        }

        log.info("ArangoDB Sink初始化完毕.");
    }

    @Override
    public void invoke(FaceProfile faceProfile, Context context) throws Exception {
        ArangoDBMark arangoDBMark = (ArangoDBMark) faceProfile;
        Integer[] beforeLabels = arangoDBMark.getBeforeLabels();
        Integer[] afterLabels = arangoDBMark.getAfterLabels();
        if (beforeLabels.length == 0 && afterLabels.length == 0) return;
        String op = arangoDBMark.getOp();
        // 明确当前流插入的是人脸聚类还是驾乘人脸聚类
        int personnelRelationType ;
        String personnelRelationName;
        if (Objects.equals(activeTable.name(), arangoConfig.getFaceProfileTable())){
            personnelRelationType = 110;
            personnelRelationName = "人和人脸聚类的关系";
        }else {
            personnelRelationType = 111;
            personnelRelationName = "人和驾乘人脸聚类的关系";
        }

        // 写入Arangodb
        Integer[] lbs = arangoDBMark.getLabels();
        String faceProfileFrom = String.format("%s/%s", activeTable.name(), arangoDBMark.getGroup());
        switch (op){
            case "c":
                // 建聚类档案和标签关系
                Relation relation = new Relation();
                relation.set_from(faceProfileFrom);
                relation.setRelation_type(LABEL_RELATION_TYPE);
                relation.setRelation_name(LABEL_RELATION_NAME);
                for (Integer lb : lbs) {
                    relation.set_to(String.format("%s/%d-1", relationEdgeCollection.name(), lb));
                    //TODO:决定使用哪种方式进行建边
//                    insertDocumentFromObject(relationEdgeCollection,relation);//不判断边连接的点是否存在
                    relationInsert(relation);//确定两边的点都存在进行建边
                }
                // 实名人员建立和人员基本信息关系
                if (arangoDBMark.getPersonnelIdType() != null && arangoDBMark.getPersonnelIdNumber() != null){
                    Relation relationPerson = new Relation();
                    relationPerson.set_from(faceProfileFrom);
                    relationPerson.set_to(String.format("%s/%s-%s",personnelInfoTable.name(),arangoDBMark.getPersonnelIdType(),arangoDBMark.getPersonnelIdNumber()));
                    relationPerson.setRelation_type(personnelRelationType);
                    relationPerson.setRelation_name(personnelRelationName);
                    relationInsert(relationPerson);
                }

                // 聚类档案写入ArangoDB
                insertDocumentFromObject(activeTable,faceProfile);

            case "u":
                String idNumber =  arangoDBMark.getPersonnelIdNumber();
                // 更新身份证号-实名处理
                if (!idNumber.isEmpty()){
                    // 建立人员信息和聚类关系
                    Relation relationPerson1 = new Relation();
                    relationPerson1.set_from(faceProfileFrom);
                    relationPerson1.set_to(String.format("%s/%s-%s",personnelInfoTable.name(),arangoDBMark.getPersonnelIdType(),arangoDBMark.getPersonnelIdNumber()));
                    relationPerson1.setRelation_type(personnelRelationType);
                    relationPerson1.setRelation_name(personnelRelationName);
                    relationInsert(relationPerson1);

                    // 补全档案信息
                    insertDocumentFromObject(activeTable,faceProfile);
                }

                // 更新标签-建立关系
                ArrayChangesResult<Integer> integerArrayChangesResult = labelsModified(beforeLabels, afterLabels);
                // 未更新标签
                if (integerArrayChangesResult.added == null && integerArrayChangesResult.removed == null){
                    insertDocumentFromObject(activeTable,faceProfileFrom);
                    return;
                }
                // 新增标签关系
                if (integerArrayChangesResult.added != null){
                    for(Integer lb: integerArrayChangesResult.added){
                        Relation relationLabel = new Relation();
                        relationLabel.set_from(faceProfileFrom);
                        relationLabel.set_to(String.format("%s/%d-1",labelTable.name(),lb));
                        relationLabel.setRelation_name(LABEL_RELATION_NAME);
                        relationLabel.setRelation_type(LABEL_RELATION_TYPE);
                        relationInsert(relationLabel);
                    }
                    insertDocumentFromObject(activeTable,faceProfile);
                }
                // 删除标签关系
                if (integerArrayChangesResult.removed != null){
                    for(Integer lb : integerArrayChangesResult.removed){
                        Relation relationLabel = new Relation();
                        relationLabel.set_from(faceProfileFrom);
                        relationLabel.set_to(String.format("%s/%d-1",labelTable.name(),lb));
                        relationLabel.setRelation_name(LABEL_RELATION_NAME);
                        relationLabel.setRelation_type(LABEL_RELATION_TYPE);
                        relationDelete(relationLabel);
                    }
                }

                // 其他更新-更新档案表
                insertDocumentFromObject(activeTable,faceProfile);

            case "d":
                // 删除所有边
                String aql = "LET relations = (FOR r IN" + relationEdgeCollection.name() +
                        "FILTER r._form == @from RETURN r._key)" +
                        "FOR relation IN relations REMOVE {_key:relation} in" + relationEdgeCollection.name();
                Map<String,Object> params = new MapBuilder()
                        .put("from",faceProfileFrom)
                        .get();
                List<String> result = queryArangoAsString(arangoDatabase, aql, params);
                log.info("删除关系边成功："+result.toString());

                //
                deleteDocument(activeTable,faceProfileFrom);
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        arangoDB.shutdown();
        log.info("ArangoDB Closed.");
    }

    public void relationInsert(Relation relation) throws Exception{
        // 依赖顶点 进行存在性判断
        String[] fromVertices = DataTransform.getDocIdToCollectionAndKey2(relation.get_from());
        String[] toVertices = DataTransform.getDocIdToCollectionAndKey2(relation.get_to());

        System.out.println(Arrays.toString(fromVertices));
        System.out.println(Arrays.toString(toVertices));
        ArangoCollection fromCollection = arangoDatabase.collection(fromVertices[0]);
        ArangoCollection toCollection = arangoDatabase.collection(toVertices[0]);

        try {
            Boolean fromExists = fromCollection.documentExists(fromVertices[1]);
            Boolean toExists = toCollection.documentExists(toVertices[1]);
            if(!fromExists) {
                log.warn("from vertices {} doesn't exist!", relation.get_from());
                return;
            }
            if(!toExists) {
                log.warn("to vertices {} doesn't exist!", relation.get_to());
                return;
            }
        } catch (ArangoDBException e) {
            log.error("ArangoDB document exists error! {}, from id: {}, to id: {}",
                    e.getErrorMessage(), relation.get_from(), relation.get_to());
            return;
        }

        // 生成时间戳
        // 生成时间戳
        int currentTimestamp = (int) (System.currentTimeMillis() / 1000);
        relation.setCreate_time(currentTimestamp);
        insertDocumentFromObject(relationEdgeCollection, relation);
    }

    public void relationDelete(Relation relation) {
        String aql = "LET relations = (FOR r IN" + relationEdgeCollection.name() +
                "FILTER r._form == @from AND r._to == @to RETURN r._key)" +
                "FOR relation IN relations REMOVE {_key:relation} in" + relationEdgeCollection.name();
        Map<String,Object> params = new MapBuilder()
                .put("from",relation.get_from())
                .put("to",relation.get_to())
                .get();
        List<String> result = queryArangoAsString(arangoDatabase, aql, params);
        log.info("删除关系边成功："+result.toString());
    }

    // 比较前后数组变化
    private static <Integer> ArrayChangesResult<Integer> labelsModified(Integer[] beforeLabels, Integer[] afterLabels){
        List<Integer> added = new ArrayList<>();
        List<Integer> removed = new ArrayList<>();

        for(Integer item:beforeLabels){
            if (contains(afterLabels, item)){
                removed.add(item);
            }
        }
        for(Integer item: afterLabels){
            if (contains(beforeLabels, item)){
                added.add(item);
            }
        }
        return new ArrayChangesResult<>(added,removed);
    }

    private static <T> boolean contains(T[] array, T item) {
        for (T element : array) {
            if (element.equals(item)) {
                return false;
            }
        }
        return true;
    }

    @Getter
    private static class ArrayChangesResult<T> {
        private final List<T> added;
        private final List<T> removed;

        public ArrayChangesResult(List<T> added, List<T> removed) {
            this.added = added;
            this.removed = removed;
        }

    }
}
