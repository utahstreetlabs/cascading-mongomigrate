/**
 Copyright 2010 BackType

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package cascading.mongomigrate.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.mongodb.DB;
import com.mongodb.Mongo;

import java.io.IOException;
import java.net.UnknownHostException;

public class MongoConfiguration {

    public static final String HOST_PROPERTY = "mapred.mongo.host";
    public static final String PORT_PROPERTY = "mapred.mongo.port";
    public static final String USERNAME_PROPERTY = "mapred.mongo.username";
    public static final String PASSWORD_PROPERTY = "mapred.mongo.password";
    public static final String INPUT_DB_NAME_PROPERTY = "mapred.mongo.input.db.name";
    public static final String INPUT_COLLECTION_NAME_PROPERTY = "mapred.mongo.input.collection.name";
    public static final String INPUT_FIELD_NAMES_PROPERTY = "mapred.mongo.input.field.names";
    public static final String PRIMARY_KEY_FIELD = "mapred.mongo.primary.key.name";
    public static final String NUM_CHUNKS = "mapred.mongo.num.chunks";

    public void configureMongo(String host, int port, String userName, String passwd) {
        job.set(HOST_PROPERTY, host);
        job.setInt(PORT_PROPERTY, port);

        if (userName != null) {
            job.set(USERNAME_PROPERTY, userName);
        }

        if (passwd != null) {
            job.set(PASSWORD_PROPERTY, passwd);
        }
    }

    public void configureMongo(String host, int port) {
        configureMongo(host, port, null, null);
    }

    public JobConf job;

    public MongoConfiguration(JobConf job) {
        this.job = job;
    }

    public Mongo getMongo() throws IOException {
        try {
            return new Mongo(job.get(MongoConfiguration.HOST_PROPERTY), job.getInt(MongoConfiguration.PORT_PROPERTY, 27017));
        } catch (UnknownHostException e) {
            throw new IOException("unable to connect to mongo", e);
        }
    }

    public DB getDB() throws IOException {
        DB db = getMongo().getDB(job.get(MongoConfiguration.INPUT_DB_NAME_PROPERTY));
        String username = job.get(MongoConfiguration.USERNAME_PROPERTY);
        String password = job.get(MongoConfiguration.USERNAME_PROPERTY);
        if (username != null && password != null) db.authenticate(username, password.toCharArray());
        return db;
    }

    public String getDBName() {
        return job.get(MongoConfiguration.INPUT_DB_NAME_PROPERTY);
    }

    public void setDBName(String dbName) {
        job.set(MongoConfiguration.INPUT_DB_NAME_PROPERTY, dbName);
    }

    public String getInputCollectionName() {
        return job.get(MongoConfiguration.INPUT_COLLECTION_NAME_PROPERTY);
    }

    public void setInputCollectionName(String collectionName) {
        job.set(MongoConfiguration.INPUT_COLLECTION_NAME_PROPERTY, collectionName);
    }

    public String[] getInputFieldNames() {
        return job.getStrings(MongoConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    }

    public void setInputFieldNames(String... fieldNames) {
        job.setStrings(MongoConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
    }

    public String getPrimaryKeyField() {
        return job.get(PRIMARY_KEY_FIELD);
    }

    public void setPrimaryKeyField(String key) {
        job.set(PRIMARY_KEY_FIELD, key);
    }

    public void setNumChunks(int numChunks) {
        job.setInt(NUM_CHUNKS, numChunks);
    }

    public int getNumChunks() {
        return job.getInt(NUM_CHUNKS, 10);
    }
}

