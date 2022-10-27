package com.bawi;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;

public class MyMapReduceMultiOutputJob extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(MyMapReduceMultiOutputJob.class);

    @Override
    public int run(String[] args) throws Exception {
        logger.info("MyMapReduceMultiOutputJob.run begin");

        Configuration conf = getConf();
        conf.set("parquet.enable.summary-metadata", "false");

        //conf.set("parquet.strings.signed-min-max.enabled", "true");
        conf.set(XmlRecordReader.XML_START_TAG, "<FWD ");
        conf.set(XmlRecordReader.XML_END_TAG, "</FWD>");

        Job job = Job.getInstance(conf, MyMapReduceMultiOutputJob.class.getName());
        job.setJarByClass(this.getClass());
        String input = args[0];
        FileInputFormat.addInputPath(job, new Path(input));
        String output = args[1];
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(BawiMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(XmlCombineFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        BawiGroupWriteSupport.setSchema(job, loadAndParseSchema("/bawi-schema.json"));
        ParquetOutputFormat.setWriteSupportClass(job, BawiGroupWriteSupport.class);
        MultipleOutputs.addNamedOutput(job, BawiGroupWriteSupport.schemaName, ParquetOutputFormat.class, Void.class, Group.class);

        job.submit();
        int statusCode = job.waitForCompletion(true) ? 0 : 1;

        logger.info("MyMapReduceMultiOutputJob.run end");

        return statusCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MyMapReduceMultiOutputJob(), args);
        System.exit(exitCode);
    }

    private String loadAndParseSchema(String schemaResource) throws IOException {
        InputStream inputStream = this.getClass().getResourceAsStream(schemaResource);
        Schema avroSchema = new Schema.Parser().parse(inputStream);
        logger.info("avroSchema=" +  avroSchema);
        MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        logger.info("parquetSchema=" +  parquetSchema);
        return parquetSchema.toString();
    }
}
