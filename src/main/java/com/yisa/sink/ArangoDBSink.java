package com.yisa.sink;

import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
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
public class ArangoDBSink extends RichSinkFunction<ArangoDBMark> {

    private static final int LABEL_RELATION_TYPE = 101;

    private static final String  LABEL_RELATION_NAME = "人和标签的关系";
    private final ConfigEntity.ArangoDB arangoConfig;
    private ArangoDB arangoDB;
    private ArangoDatabase arangoDatabase;
    private ArangoCollection relationEdgeCollection;

    private ArangoCollection activeTable;

    private  ArangoCollection personnelInfoTable;
    private  ArangoCollection labelTable;

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
    public void invoke(ArangoDBMark arangoDBMark, Context context) {
        FaceProfile faceProfile = arangoDBMark.getFaceProfile();
        Integer[] beforeLabels = arangoDBMark.getBeforeLabels();
        Integer[]  afterLabels = arangoDBMark.getAfterLabels();

        String beforePersonnelIdType = arangoDBMark.getBeforePersonnelIdType();
        String beforePersonnelIdNumber = arangoDBMark.getBeforePersonnelIdNumber();
        String afterPersonnelIdType = arangoDBMark.getAfterPersonnelIdType();
        String afterPersonnelIdNumber = arangoDBMark.getAfterPersonnelIdNumber();

        // 聚类在ArangoDB的顶点格式
        String faceProfileFrom = String.format("%s/%s", activeTable.name(), arangoDBMark.getGroup());

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

        try {
            // 先处理人员档案
            if (!beforePersonnelIdNumber.isEmpty() && !beforePersonnelIdType.isEmpty()
                    && afterPersonnelIdNumber.isEmpty() && afterPersonnelIdType.isEmpty()){
                log.info("Delete profile");
                // 删除实名人和人员基本信息关系
                Relation relationPerson = new Relation();
                relationPerson.set_from(faceProfileFrom);
                relationPerson.set_to(String.format("%s/%s-%s",personnelInfoTable.name(), beforePersonnelIdType, beforePersonnelIdNumber));
                relationPerson.setRelation_type(personnelRelationType);
                relationPerson.setRelation_name(personnelRelationName);
                relationDelete(relationPerson);
            }else if(beforePersonnelIdNumber.isEmpty() && beforePersonnelIdType.isEmpty()
                    && !afterPersonnelIdNumber.isEmpty() && !afterPersonnelIdType.isEmpty()){
                // 新建人和人员基本信息关系
                log.info("Create profile");
                // 聚类档案写入ArangoDB
                activeTable.insertDocument(formatArangoDB(faceProfile));
                Relation relationPerson = new Relation();
                relationPerson.set_from(faceProfileFrom);
                relationPerson.set_to(String.format("%s/%s-%s",personnelInfoTable.name(), afterPersonnelIdType, afterPersonnelIdNumber));
                relationPerson.setRelation_type(personnelRelationType);
                relationPerson.setRelation_name(personnelRelationName);
                relationInsert(relationPerson);
            }else if (!beforePersonnelIdNumber.isEmpty() && !beforePersonnelIdType.isEmpty()
                    && !afterPersonnelIdNumber.isEmpty() && !afterPersonnelIdType.isEmpty()
                    && !beforePersonnelIdNumber.equals(afterPersonnelIdNumber)){
                // 更新实名人和人员基本信息关系
                log.info("Update profile");
                Relation beforeRelationPerson = new Relation();
                beforeRelationPerson.set_from(faceProfileFrom);
                beforeRelationPerson.set_to(String.format("%s/%s-%s",personnelInfoTable.name(), beforePersonnelIdType, beforePersonnelIdNumber));
                beforeRelationPerson.setRelation_type(personnelRelationType);
                beforeRelationPerson.setRelation_name(personnelRelationName);
                relationDelete(beforeRelationPerson);

                Relation AfterRelationPerson = new Relation();
                AfterRelationPerson.set_from(faceProfileFrom);
                AfterRelationPerson.set_to(String.format("%s/%s-%s",personnelInfoTable.name(), afterPersonnelIdType, afterPersonnelIdNumber));
                AfterRelationPerson.setRelation_type(personnelRelationType);
                AfterRelationPerson.setRelation_name(personnelRelationName);
                relationInsert(AfterRelationPerson);
            }
        }catch (Exception e){
            log.error(String.valueOf(e));
        }


        // 判断标签
        if (beforeLabels.length == 0 && afterLabels.length == 0 ) {
            log.info("No labels,Skip");
            return;
        }

        // 根据op内容操作ArangoDB
        String op = arangoDBMark.getOp();
        try {
            switch (op){
                case "c":
                    log.info("Execute create process");
                    // 建聚类档案和标签关系
                    Relation relation = new Relation();
                    relation.set_from(faceProfileFrom);
                    relation.setRelation_type(LABEL_RELATION_TYPE);
                    relation.setRelation_name(LABEL_RELATION_NAME);
                    for (Integer lb : afterLabels) {
                        relation.set_to(String.format("%s/%d-1", labelTable.name(), lb));
                        relationInsert(relation);//确定两边的点都存在进行建边
                    }
                    break;
                case "u":
                    log.info("Execute update process");
                    // 未实名聚类第一次被打标签(因ArangoDB不入全量数据，所以第一次打标签时才在ArangoBD中创建聚类)
                    if (beforeLabels.length == 0 && afterPersonnelIdType.isEmpty() && afterPersonnelIdNumber.isEmpty()){
                        // 聚类档案写入ArangoDB
                        activeTable.insertDocument(formatArangoDB(faceProfile));
                        // 建聚类档案和标签关系
                        Relation relationFirsts = new Relation();
                        relationFirsts.set_from(faceProfileFrom);
                        relationFirsts.setRelation_type(LABEL_RELATION_TYPE);
                        relationFirsts.setRelation_name(LABEL_RELATION_NAME);
                        for (Integer lb : afterLabels) {
                            relationFirsts.set_to(String.format("%s/%d-1", labelTable.name(), lb));
                            relationInsert(relationFirsts);
                        }
                        return;
                    }

                    // 更新标签-建立关系
                    ArrayChangesResult<Integer> integerArrayChangesResult = labelsModified(beforeLabels, afterLabels);
                    // 新增标签关系
                    if (integerArrayChangesResult.added != null && !integerArrayChangesResult.added.isEmpty()){
                        log.info("Add labels");
                        for(Integer lb: integerArrayChangesResult.added){
                            Relation relationLabel = new Relation();
                            relationLabel.set_from(faceProfileFrom);
                            relationLabel.set_to(String.format("%s/%d-1",labelTable.name(),lb));
                            relationLabel.setRelation_name(LABEL_RELATION_NAME);
                            relationLabel.setRelation_type(LABEL_RELATION_TYPE);
                            relationInsert(relationLabel);
                        }
                    }
                    // 删除标签关系
                    if (integerArrayChangesResult.removed != null && !integerArrayChangesResult.removed.isEmpty()){
                        log.info("Remove labels");
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
                    updateDocumentFromBase(activeTable,formatArangoDB(faceProfile));
                    break;

                case "d":
                    log.info("Execute remove process");
                    String aql = "LET relations = (FOR r IN " + relationEdgeCollection.name() +
                            " FILTER r._from == @from RETURN r._key)" +
                            " FOR relation IN relations REMOVE {_key:relation} in " + relationEdgeCollection.name();
                    Map<String,Object> params = new MapBuilder()
                            .put("from",faceProfileFrom)
                            .get();
                    queryArangoAsString(arangoDatabase, aql, params);

                    deleteDocument(activeTable,String.valueOf(arangoDBMark.getGroup()));
                    break;
            }
        }catch (Exception e){
            log.error(String.valueOf(e));
        }


    }

    @Override
    public void close() throws Exception {
        super.close();
        arangoDB.shutdown();
        log.info("ArangoDB Closed.");
    }

    public void relationInsert(Relation relation) {
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
        String aql = "LET relations = (FOR r IN " + relationEdgeCollection.name() +
                " FILTER r._from == @from AND r._to == @to RETURN r._key)" +
                " FOR relation IN relations REMOVE {_key:relation} in " + relationEdgeCollection.name();
        Map<String,Object> params = new MapBuilder()
                .put("from",relation.get_from())
                .put("to",relation.get_to())
                .get();
        List<String> result = queryArangoAsString(arangoDatabase, aql, params);
        log.info("Delete relation success {} _form:{},_to:{}:",result,relation.get_from(),relation.get_to());
    }

    // 比较前后数组变化
    private static ArrayChangesResult<Integer> labelsModified(Integer[] beforeLabels, Integer[] afterLabels){
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

    private static BaseDocument formatArangoDB(FaceProfile faceProfile){
        BaseDocument baseDocument = new BaseDocument();
        baseDocument.setKey(String.valueOf(faceProfile.getGroup()));
        baseDocument.addAttribute("group",faceProfile.getGroup());
        baseDocument.addAttribute("center",faceProfile.getCenter());
        baseDocument.addAttribute("new_id",faceProfile.getNewId());
        baseDocument.addAttribute("group_count",faceProfile.getGroupCount());
        baseDocument.addAttribute("face_count",faceProfile.getFaceCount());
        baseDocument.addAttribute("person_count",faceProfile.getPersonCount());
        baseDocument.addAttribute("personnel_photo_url",faceProfile.getPersonnelPhotoUrl());
        baseDocument.addAttribute("personnel_id_number",faceProfile.getPersonnelIdNumber());
        baseDocument.addAttribute("personnel_id_type",faceProfile.getPersonnelIdType());
        baseDocument.addAttribute("personnel_name",faceProfile.getPersonnelName());
        baseDocument.addAttribute("cosine_similarity",faceProfile.getCosineSimilarity());
        baseDocument.addAttribute("associated_time",faceProfile.getAssociatedTime());
        baseDocument.addAttribute("insert_time",faceProfile.getInsertTime());
        baseDocument.addAttribute("update_time",faceProfile.getInsertTime());
        baseDocument.addAttribute("device_object_types",faceProfile.getSourceTypes());
        baseDocument.addAttribute("centers",faceProfile.getCenters());
        baseDocument.addAttribute("household_code",faceProfile.getHouseholdCode());
        baseDocument.addAttribute("household_address",faceProfile.getHouseholdAddress());
        baseDocument.addAttribute("birthday",faceProfile.getBirthday());
        baseDocument.addAttribute("age_id",faceProfile.getAgeId());
        baseDocument.addAttribute("gender",faceProfile.getGender());
        baseDocument.addAttribute("high_quality_id",faceProfile.getHighQualityId());
        return baseDocument;
    }
}
