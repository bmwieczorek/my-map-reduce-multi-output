package com.bawi.mrunit;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.internal.output.MockMultipleOutputs;

import java.io.IOException;

class MockVoidMultipleOutputs<K, V> extends MockMultipleOutputs<K, V> {

	@SuppressWarnings("rawtypes")
	MockVoidMultipleOutputs(TaskInputOutputContext context) {
		super(context);
	}
	
	@Override
	@SuppressWarnings({ "unchecked"})
	public <KK, VV> void write(String namedOutput, KK key, VV value,
							   String baseOutputPath) throws IOException, InterruptedException {
		if (key == null) {
			key = (KK) NullWritable.get();
		}
		super.write(namedOutput, key, value, baseOutputPath);
	}

}
