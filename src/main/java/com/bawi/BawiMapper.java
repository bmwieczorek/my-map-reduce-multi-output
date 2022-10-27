package com.bawi;

import org.apache.avro.Schema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class BawiMapper extends Mapper<LongWritable, Text, Void, Group> {

    private static Logger logger = LoggerFactory.getLogger(BawiMapper.class);
    private MultipleOutputs<Void, Group> mos;
    private SimpleGroupFactory bawiSimpleGroupFactory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("setup [START]");
        MessageType bawiSchema = loadParquetSchema("/bawi-schema.json");
        logger.info("setup bawiSchema=" +  bawiSchema);
        bawiSimpleGroupFactory = new SimpleGroupFactory(bawiSchema);
        mos = new MultipleOutputs<>(context);
        logger.info("setup [END]");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("map [START]");
        int fileIndex = (int) key.get();
        logger.info("map fileIndex=" + fileIndex);
        String data = new String(value.getBytes());
        logger.info("map data=" + data);
        data = data.substring("<FWD >".length(), data.indexOf("</FWD>"));
        logger.info("map data=" + data);
        Group group = bawiSimpleGroupFactory.newGroup();
        String id = data.substring(0, data.indexOf(","));
        logger.info("map id=" + id);
        if (id.equals("null")) {
            group.add("idint", -1);
            group.add("idstring", "-1");
        } else {
            group.add("idint", Integer.parseInt(id));
            group.add("idintnullable", Integer.parseInt(id));
            group.add("idstring", id);
            group.add("idstringnullable", id);
        }

        String name = data.substring(data.indexOf(",") + 1);
        logger.info("map name=" + name);
        if (name.equals("null")) {
            group.add("namestring", "null-value");

        } else {
            group.add("namestring", name);
            group.add("namestringnullable", name);
        }
        mos.write("bawi", null, group, "bawi");
        logger.info("map [END]");

    }

    private MessageType loadParquetSchema(String schemaName) throws IOException {
        logger.info("loading schemaName=" + schemaName);
        InputStream inputStream = this.getClass().getResourceAsStream(schemaName);
        Schema avroSchema = new Schema.Parser().parse(inputStream);
        logger.info("avroSchema=" + avroSchema);
        MessageType schemaType = new AvroSchemaConverter().convert(avroSchema);
        logger.info("parquet schemaType=" + schemaType);
        return schemaType;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("cleanup [START]");
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
        logger.info("cleanup [DONE]");
    }

}
