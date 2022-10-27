package com.bawi.mrunit.parquet;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.parquet.example.data.Group;

public class ParquetSerialization implements Serialization<Group> {

	@Override
	public boolean accept(Class<?> c) {
		return Group.class.isAssignableFrom(c);
	}

	@Override
	public Serializer<Group> getSerializer(Class<Group> c) {
		return new ParquetSerializer();
	}

	@Override
	public Deserializer<Group> getDeserializer(Class<Group> c) {
		return new ParquetDeserializer();
	}

}
