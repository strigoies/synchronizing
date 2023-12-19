package com.yisa;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.yisa.model.Relation;
import com.yisa.sink.ArangoDBSink;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class ArangoDBTest {

    @Test
    public void showArangoDBConfig(){
        ConfigEntity config = ReadConfig.getConfigEntity();
        config.getArangoDB().setActiveTable(config.getArangoDB().getFaceProfileTable());
        System.out.println(config.getArangoDB().getDatabase());
        System.out.println(config.getArangoDB().getActiveTable());
    }

    @Test
    public void lightningDBConfig(){
        ConfigEntity config = ReadConfig.getConfigEntity();
        config.getParameter().setJobName("faceProfileSynchronizing");
        ConfigEntity.LightningDB lightningDB = config.getLightningDB();
        System.out.println(lightningDB.toString());
        // 随机获取一个节点
        List<List> lightningDBHostPorts = lightningDB.getHosts();
        int index = new Random().nextInt(lightningDBHostPorts.size());
        String url = String.format("jdbc:clickhouse://%s:%s/%s?use_binary_string=true",
                lightningDBHostPorts.get(index).get(0),
                lightningDBHostPorts.get(index).get(1),
                lightningDB.getDatabase()); // 默认数据库
        System.out.println(url);
    }

    @Test
    public void updateArangoDBDocument(){
        ConfigEntity config = ReadConfig.getConfigEntity();
        config.getParameter().setJobName("faceProfileSynchronizing");

        ConfigEntity.ArangoDB arangoConfig = config.getArangoDB();
        config.getArangoDB().setActiveTable(config.getArangoDB().getFaceProfileTable());

        List<List> arangoDBHostPorts = arangoConfig.getHosts();
        int index = new Random().nextInt(arangoDBHostPorts.size());

        ArangoDB arangoDB = new ArangoDB.Builder()
                .host((String) arangoDBHostPorts.get(index).get(0), (Integer) arangoDBHostPorts.get(index).get(1))
                .user(arangoConfig.getUsername()).password(arangoConfig.getPassword())
                .build();
        ArangoDatabase arangoDatabase = arangoDB.db(arangoConfig.getDatabase());
        ArangoCollection personnelInfoTable = arangoDatabase.collection(arangoConfig.getPersonnelInfoTable());

        String documentKey = "111-249741919473551784";
        BaseDocument updatedDocument = new BaseDocument();
        updatedDocument.addAttribute("age",12);
        updatedDocument.addAttribute("name","test");

        System.out.println(updatedDocument);
        try {
            personnelInfoTable.updateDocument(documentKey,updatedDocument);
        }catch (ArangoDBException e){
            e.printStackTrace();
        }finally {
            arangoDB.shutdown();
        }

    }

    @Test
    public void relationInsertTest() throws Exception {
        ConfigEntity config = ReadConfig.getConfigEntity();
        config.getParameter().setJobName("faceProfileSynchronizing");

        ConfigEntity.ArangoDB arangoConfig = config.getArangoDB();
        config.getArangoDB().setActiveTable(config.getArangoDB().getFaceProfileTable());

        List<List> arangoDBHostPorts = arangoConfig.getHosts();
        int index = new Random().nextInt(arangoDBHostPorts.size());

        ArangoDB arangoDB = new ArangoDB.Builder()
                .host((String) arangoDBHostPorts.get(index).get(0), (Integer) arangoDBHostPorts.get(index).get(1))
                .user(arangoConfig.getUsername()).password(arangoConfig.getPassword())
                .build();
        ArangoDatabase arangoDatabase = arangoDB.db(arangoConfig.getDatabase());
        ArangoCollection personnelInfoTable = arangoDatabase.collection(arangoConfig.getPersonnelInfoTable());

        Relation relation = new Relation();

        relation.set_from("personnel_info/111-946915967635910579");
        relation.set_to("label_info/1-1");
        relation.setDescribe("flink-arangoDB测试");


    }
}
