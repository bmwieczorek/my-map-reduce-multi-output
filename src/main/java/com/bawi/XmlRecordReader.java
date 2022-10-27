package com.bawi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;

public class XmlRecordReader extends RecordReader<LongWritable, Text> {

	private static Logger logger = LoggerFactory.getLogger(XmlRecordReader.class);

	public static String XML_START_TAG = "xml.start.tag";
	public static String XML_END_TAG = "xml.end.tag";

	private String xmlStartTag;
	private String xmlEndTag;

	private long start;

	private long end;

	private FSDataInputStream fsin;

	private Decompressor decompressor;

	private CompressionCodecFactory compressionCodecs;

	private CompressionCodec codec;

	private Seekable filePosition;

	private XmlInputStream fwdis;

	private Text value = new Text();

	private LongWritable key;

	XmlRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
		Configuration conf = context.getConfiguration();
		xmlStartTag = conf.get(XML_START_TAG);
		xmlEndTag = conf.get(XML_END_TAG);
		logger.info("xmlStartTag=" + xmlStartTag);
		logger.info("xmlEndTag=" + xmlEndTag);
		key = new LongWritable(index);
		Path file = split.getPath(index);
		compressionCodecs = new CompressionCodecFactory(conf);
		codec = compressionCodecs.getCodec(file);

		start = split.getOffset(index);
		end = start + split.getLength(index);

		FileSystem fs = file.getFileSystem(conf);

		fsin = fs.open(file);

		if (isCompressedInput()) {
			logger.info("Configuring compressed input");
			decompressor = CodecPool.getDecompressor(codec);

			fwdis = new XmlInputStream(
					new BufferedInputStream(codec.createInputStream(fsin, decompressor), 128 * 1024),
					xmlStartTag, xmlEndTag);
			filePosition = fsin;
		} else {
			logger.info("Configuring uncompressed input. Start position = {}, end = {} ", start, end);
			fwdis = new XmlInputStream(new BufferedInputStream(fsin), xmlStartTag, xmlEndTag);
			filePosition = fsin;
		}

	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) {

	}

	@Override
	public boolean nextKeyValue() throws IOException {
		// instead of position in file we return file index
//		 key.set(fsin.getPos());
		 fsin.getPos();
		 String xml = fwdis.readFwdAsString();
		 if (xml != null) {
			 value.set(xml);
			 return true;
		 }
		return false;
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return (getPos() - start) / (float) (end - start);
	}

	public long getPos() throws IOException {
		return filePosition.getPos();
	}

	@Override
	public void close() throws IOException {
		try {
			if (fwdis != null) {
				fwdis.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}
		}
	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

}
