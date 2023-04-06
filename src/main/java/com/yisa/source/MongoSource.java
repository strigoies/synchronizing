package com.yisa.source;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.yisa.model.FaceProfile;
import com.yisa.model.FaceProfileSchema;
import com.yisa.utils.ConfigEntity;

public class MongoSource {
    public static MongoDBSource<FaceProfile> getMongoSource(ConfigEntity.MongoDB mongodb) {
        MongoDBSource<FaceProfile> mongoSource =
                MongoDBSource.<FaceProfile>builder()
                        .hosts(mongodb.getHosts())
                        .databaseList(mongodb.getDatabase())
                        .collectionList(mongodb.getDatabase() + "." + mongodb.getCollection())
                        .username(mongodb.getUsername())
                        .password(mongodb.getPassword())
                        .deserializer(new FaceProfileSchema())
                        .build();

        return mongoSource;
    }
}
