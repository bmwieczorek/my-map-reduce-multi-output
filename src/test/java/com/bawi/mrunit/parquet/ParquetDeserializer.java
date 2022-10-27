package com.bawi.mrunit.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.parquet.Closeables;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

class ParquetDeserializer implements Deserializer<Group> {

    private static Logger logger = LoggerFactory.getLogger(ParquetDeserializer.class);

    private static MessageType bawiSchema;

    private boolean loggingEnabled = false;

    static {
        try {
            logger.info("Loading Parquet schemas");
            bawiSchema = loadSchema("/bawi-schema.json");
  ; } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private ObjectInputStream ois;

    @Override
    public void open(InputStream in) throws IOException {
        ois = new ObjectInputStream(in);
    }

    private void readField(Type fieldType, int fieldIdx, Group parentGroup) throws ClassNotFoundException, IOException {
        if (loggingEnabled) {
            logger.info("DESER:Field type:" + fieldType.toString());
        }
        if (fieldType.isPrimitive()) {
            if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
                readArrayOfPrimitives(fieldType, parentGroup);
            } else {
                boolean valueExists = (Boolean) ois.readObject();
                if (loggingEnabled) {
                    logger.info("DESER:Value exists:" + (valueExists ? "YES" : "NO") + ",Field:" + fieldType.getName());
                }
                if (valueExists) {
                    readPrimitive(parentGroup, fieldType, fieldIdx);
                }
            }
        } else {
            if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
                readArrayOfRecords(fieldType, parentGroup);
            } else {
                boolean valueExists = (Boolean) ois.readObject();
                if (loggingEnabled) {
                    logger.info("DESER:Value exists:" + (valueExists ? "YES" : "NO") + ",Field:" + fieldType.getName());
                }
                if (valueExists) {
                    readRecord(fieldType, fieldIdx, parentGroup);
                }
            }
        }
    }

    private void readRecord(Type type, int fieldIdx, Group parentGroup) throws ClassNotFoundException, IOException {
        Group recordGroup = parentGroup.addGroup(fieldIdx);
        int fieldCount = type.asGroupType().getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            Type fieldType = type.asGroupType().getType(i);
            boolean valueExists = (Boolean) ois.readObject();
            if (loggingEnabled) {
                logger.info("DESER:Value exists:" + (valueExists ? "YES" : "NO") + ",Field:" + fieldType.getName());
            }
            if (valueExists) {
                readField(fieldType, i, recordGroup);
            }
        }
    }

    @Override
    public Group deserialize(Group record) throws IOException {
        String recordType = ois.readUTF();
        MessageType schema = getRecordSchema(recordType);
        if (schema == null) {
            throw new IOException("No schema for given record type: " + recordType);
        }
        try {
            Group result = new SimpleGroup(schema);
            int fieldCount = schema.getFieldCount();
            for (int fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++) {
                Type fieldType = schema.getType(fieldIdx);
                if (loggingEnabled) {
                    logger.info("DESER:" + recordType + ",Field:" + fieldIdx + "/" + (fieldCount - 1) + "," + fieldType.getName());
                }
                boolean valueExists = (Boolean) ois.readObject();
                if (loggingEnabled) {
                    logger.info("DESER:Value exists:" + (valueExists ? "YES" : "NO") + ",Field:" + fieldType.getName());
                }
                if (valueExists) {
                    readField(fieldType, fieldIdx, result);
                }
            }
            if (loggingEnabled) {
                logger.info("DESER:" + recordType + " DUMP START ####");
                logger.info(result.toString());
                logger.info("DESER:" + recordType + " DUMP END ####");
            }
            return result;
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    private void readArrayOfRecords(Type type, Group parentGroup) throws IOException, ClassNotFoundException {
        Integer recordCount = (Integer) ois.readObject();
        if (recordCount == null) {
            throw new RuntimeException("Record array size expected");
        }
        for (int j = 0; j < recordCount; j++) {
            readRecord(type.asGroupType(), 0, parentGroup);
        }
    }

    private void readArrayOfPrimitives(Type type, Group parentGroup) throws IOException, ClassNotFoundException {
        Integer valueCount = (Integer) ois.readObject();
        if (valueCount == null) {
            throw new RuntimeException("Primitive array size expected");
        }
        for (int k = 0; k < valueCount; k++) {
            readPrimitive(parentGroup, type.asPrimitiveType(), 0);
        }
    }

    private void readPrimitive(Group record, Type type, int index) throws IOException, ClassNotFoundException {
        PrimitiveTypeName fieldType = type.asPrimitiveType().getPrimitiveTypeName();
        Object value = ois.readObject();
        if (value == null) {
            logger.info("DESER:No value for primitive");
            return;
        }
        switch (fieldType) {
            case INT32: {
                Integer intValue = (Integer) value;
                if (loggingEnabled) {
                    logger.info("DESER:Reading primitive integer:" + intValue);
                }
                record.add(index, intValue);
                break;
            }
            case INT64: {
                Long longValue = (Long) value;
                if (loggingEnabled) {
                    logger.info("DESER:Reading primitive long:" + longValue);
                }
                record.add(index, longValue);
                break;
            }
            case BINARY: {
                String stringValue = (String) value;
                if (loggingEnabled) {
                    logger.info("DESER:Reading primitive string:" + stringValue);
                }
                record.add(index, stringValue);
                break;
            }
            case DOUBLE: {
                Double doubleValue = (Double) value;
                if (loggingEnabled) {
                    logger.info("DESER:Reading primitive double:" + doubleValue);
                }
                record.add(index, doubleValue);
                break;
            }
            case BOOLEAN: {
                Boolean booleanValue = (Boolean) value;
                if (loggingEnabled) {
                    logger.info("DESER:Reading primitive boolean:" + booleanValue);
                }
                record.add(index, booleanValue);
                break;
            }
            default:
                logger.error("Unknown field type: " + fieldType.name());
                break;
        }
    }

    @Override
    public void close() throws IOException {
        Closeables.close(ois);
    }

    private MessageType getRecordSchema(String recordType) {
        if ("bawi".equals(recordType)) {
            return bawiSchema;
        }
        return null;
    }

    private static MessageType loadSchema(String schemaName) throws IOException {
        InputStream inputStream = ParquetDeserializer.class.getResourceAsStream(schemaName);
        Schema avroSchema = new Schema.Parser().parse(inputStream);
        return new AvroSchemaConverter().convert(avroSchema);
    }
}
