/**
 Copyright 2010 BackType

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package cascading.mongomigrate.hadoop;

import cascading.tuple.Tuple;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.bson.types.ObjectId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

public class MongoInputFormat implements InputFormat<BytesWritable, TupleWrapper> {
    private static final int OID_BYTES = 12;

    private static final Logger LOG = LoggerFactory.getLogger(MongoInputFormat.class);

    public static class MongoRecordReader implements RecordReader<BytesWritable, TupleWrapper> {


        private MongoConfiguration conf;
        private DBCursor cursor;
        private Statement statement;
        private Mongo mongo;
        private MongoInputSplit split;
        private long pos = 0;
        private String[] fieldNames;

        protected MongoRecordReader(MongoInputSplit split, JobConf job) throws IOException {
            this.split = split;
            conf = new MongoConfiguration(job);
            fieldNames = conf.getInputFieldNames();
            cursor = executeQuery(conf.getDB(), conf, split);
        }

        protected DBCursor executeQuery(DB db, MongoConfiguration conf, MongoInputSplit split) {
            BasicDBObject query = new BasicDBObject(conf.getPrimaryKeyField(),
              new BasicDBObject("$gte", new ObjectId(split.startId.getBytes())).append("$lt", new ObjectId(split.endId.getBytes())));
            BasicDBObject keys = new BasicDBObject();
            for (String key : conf.getInputFieldNames()) {
                keys.put(key, 1);
            }
            return db.getCollection(conf.getInputCollectionName()).find(query, keys);
        }

        public void close() throws IOException {
            conf.getMongo().close();
        }

        /** {@inheritDoc} */
        public BytesWritable createKey() {
            return new BytesWritable(new byte[OID_BYTES]);
        }

        /** {@inheritDoc} */
        public TupleWrapper createValue() {
            return new TupleWrapper();
        }

        /** {@inheritDoc} */
        public long getPos() throws IOException {
            return pos;
        }

        /** {@inheritDoc} */
        public float getProgress() throws IOException {
            return (pos / (float) split.getLength());
        }

        /** {@inheritDoc} */
        public boolean next(BytesWritable key, TupleWrapper value) throws IOException {
            if (!cursor.hasNext()) {
                return false;
            }
            DBObject curr = cursor.next();
            key.set(((ObjectId) curr.get(split.primaryKeyField)).toByteArray(), 0, OID_BYTES);

            value.tuple = new Tuple();

            for (String name : fieldNames) {
                Object o = curr.get(name);
                if (o instanceof byte[]) {
                    o = new BytesWritable((byte[]) o);
                } else if (o instanceof BigInteger) {
                    o = ((BigInteger) o).longValue();
                } else if (o instanceof BigDecimal) {
                    o = ((BigDecimal) o).doubleValue();
                }
                try {
                    value.tuple.add(o);
                } catch (Throwable t) {
                    LOG.info("WTF: " + o.toString() + o.getClass().toString());
                    throw new RuntimeException(t);
                }
            }
            pos++;
            return true;
        }
    }

    protected static class MongoInputSplit implements InputSplit {

        public BytesWritable startId = null;
        public BytesWritable endId = null;
        public String primaryKeyField;
        private long length;

        public MongoInputSplit() {
        }

        public MongoInputSplit(BytesWritable start, BytesWritable end, String primaryKeyField, long length) {
            startId = start;
            endId = end;
            this.primaryKeyField = primaryKeyField;
            length = length;
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public long getLength() throws IOException {
            return length;
        }

        public void readFields(DataInput input) throws IOException {
            startId = new BytesWritable(new byte[OID_BYTES]);
            startId.readFields(input);
            endId = new BytesWritable(new byte[OID_BYTES]);
            endId.readFields(input);
            primaryKeyField = WritableUtils.readString(input);
        }

        public void write(DataOutput output) throws IOException {
            startId.write(output);
            endId.write(output);
            WritableUtils.writeString(output, primaryKeyField);
        }
    }

    public RecordReader<BytesWritable, TupleWrapper> getRecordReader(InputSplit split, JobConf job,
        Reporter reporter) throws IOException {
        return new MongoRecordReader((MongoInputSplit) split, job);
    }

    private long getRangeCount(DBCollection collection, String field, ObjectId minId, ObjectId maxId) {
        BasicDBObject query = new BasicDBObject();
        query.put(field, new BasicDBObject("$gte", minId).append("$lt", maxId));
        return collection.count(query);
    }

    private BigInteger getMaxId(DBCollection collection, String field) {
        ObjectId id = (ObjectId) collection.find().sort(new BasicDBObject(field, -1)).next().get(field);
        return new BigInteger(id.toByteArray());
    }

    private BigInteger getMinId(DBCollection collection, String field) {
        ObjectId id = (ObjectId) collection.find().sort(new BasicDBObject(field, 1)).next().get(field);
        return new BigInteger(id.toByteArray());
    }

    public InputSplit[] getSplits(JobConf job, int ignored) throws IOException {
        MongoConfiguration conf = new MongoConfiguration(job);
        try {
            int chunks = conf.getNumChunks();
            DB db = conf.getDB();
            DBCollection collection = db.getCollection(conf.getInputCollectionName());
            String primaryKeyField = conf.getPrimaryKeyField();
            BigInteger maxId = getMaxId(collection, primaryKeyField).add(BigInteger.ONE);
            BigInteger minId = getMinId(collection, primaryKeyField);
            BigInteger chunkSize = maxId.subtract(minId).add(BigInteger.ONE).
                divide(BigInteger.valueOf(chunks).add(BigInteger.ONE));
            chunks = maxId.subtract(minId).add(BigInteger.ONE).divide(chunkSize).intValue();
            InputSplit[] ret = new InputSplit[chunks];

            BigInteger currId = minId;
            for (int i = 0; i < chunks; i++) {
                BigInteger start = currId;
                currId = currId.add(chunkSize);
                long splitSize = getRangeCount(
                    collection, primaryKeyField, new ObjectId(start.toByteArray()), new ObjectId(currId.toByteArray()));

                LOG.info(String.format("creating split %d of %d on %s of size %d; %s gte %s; %s lt %s",
                    new Object[] { i + 1, chunks, conf.getInputCollectionName(), splitSize,
                    primaryKeyField, new ObjectId(start.toByteArray()), primaryKeyField, new ObjectId(currId.toByteArray())}));

                ret[i] = new MongoInputSplit(new BytesWritable(start.toByteArray()), new BytesWritable(currId.toByteArray()),
                                             primaryKeyField, splitSize);
            }
            return ret;
        } finally {
            conf.getMongo().close();
        }
    }

    public static void setInput(JobConf job, int numChunks, String host, int port, String username, String pwd,
                                String dbName, String collectionName, String pkField,
        String... fieldNames) {
        job.setInputFormat(MongoInputFormat.class);

        MongoConfiguration dbConf = new MongoConfiguration(job);
        dbConf.configureMongo(host, port, username, pwd);
        dbConf.setDBName(dbName);
        dbConf.setInputCollectionName(collectionName);
        dbConf.setInputFieldNames(fieldNames);
        dbConf.setPrimaryKeyField(pkField);
        dbConf.setNumChunks(numChunks);
    }
}
