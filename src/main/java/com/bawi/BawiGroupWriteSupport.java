package com.bawi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class BawiGroupWriteSupport extends WriteSupport<Group> {

    public static final String schemaName = "bawi";

    public static void setSchema(Job job,String schema) {
        Configuration configuration = ContextUtil.getConfiguration(job);
        configuration.set(schemaName, schema);
    }

    public static MessageType getSchema(Configuration configuration) {
        return parseMessageType(checkNotNull(configuration.get(schemaName), schemaName));
    }

    private MessageType schema = null;
    private GroupWriter groupWriter;

    public BawiGroupWriteSupport() {
    }

    BawiGroupWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    @Override
    public void write(Group record) {
        groupWriter.write(record);
    }

}
