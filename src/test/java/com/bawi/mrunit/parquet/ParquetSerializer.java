package com.bawi.mrunit.parquet;

import org.apache.hadoop.io.serializer.Serializer;
import org.apache.parquet.Closeables;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

class ParquetSerializer implements Serializer<Group> {

    private static Logger logger = LoggerFactory.getLogger(ParquetSerializer.class);

    private ObjectOutputStream oos;

    private boolean loggingEnabled = false;

    @Override
    public void open(OutputStream out) throws IOException {
        oos = new ObjectOutputStream(out);
    }

    private void writeField(Type fieldType, int fieldIdx, Group parentGroup) throws IOException {
        if (loggingEnabled) {
            logger.info("SER:Field type:" + fieldType.toString());
        }
        if (fieldType.isPrimitive()) {
            if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
                writeArrayOfPrimitives(fieldIdx, parentGroup);
            } else {
                int repCount = parentGroup.getFieldRepetitionCount(fieldIdx);
                if (repCount > 0) {
                    if (loggingEnabled) {
                        logger.info("SER:Writing value for primitive:" + fieldType.getName());
                    }
                    oos.writeObject(Boolean.TRUE);
                    writePrimitive(parentGroup, fieldType, fieldIdx, 0);
                } else {
                    if (loggingEnabled) {
                        logger.info("SER:Writing NULL for primitive:" + fieldType.getName());
                    }
                    oos.writeObject(Boolean.FALSE);
                }
            }
        } else {
            Group fieldGroup = parentGroup.getGroup(fieldIdx, 0);
            GroupType fieldGroupType = fieldGroup.getType();
            if (fieldGroupType.isRepetition(Type.Repetition.REPEATED)) {
                writeArrayOfRecords(fieldIdx, parentGroup);
            } else {
                int repCount = parentGroup.getFieldRepetitionCount(fieldIdx);
                if (repCount > 0) {
                    if (loggingEnabled) {
                        logger.info("SER:Writing value for record:" + fieldType.getName());
                    }
                    oos.writeObject(Boolean.TRUE);
                    writeRecord(fieldGroup);
                } else {
                    if (loggingEnabled) {
                        logger.info("SER:Writing NULL for record:" + fieldType.getName());
                    }
                    oos.writeObject(Boolean.FALSE);
                }
            }
        }
    }

    private void writeRecord(Group parentGroup) throws IOException {
        GroupType groupType = parentGroup.getType();
        int fieldCount = groupType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            Type fieldType = groupType.getType(i);
            int repCount = parentGroup.getFieldRepetitionCount(i);
            if (repCount > 0) {
                if (loggingEnabled) {
                    logger.info("SER:Writing value for field:" + fieldType.getName());
                }
                oos.writeObject(Boolean.TRUE);
                writeField(fieldType, i, parentGroup);
            } else {
                if (loggingEnabled) {
                    logger.info("SER:No value for field:" + fieldType.getName());
                }
                oos.writeObject(Boolean.FALSE);
            }
        }
    }

    @Override
    public void serialize(Group record) throws IOException {
        GroupType schema = record.getType();
        String recordType = schema.getName();
        oos.writeUTF(recordType);
        int fieldCount = schema.getFieldCount();
        for (int fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++) {
            Type fieldType = schema.getType(fieldIdx);
            if (loggingEnabled) {
                logger.info("SER:" + recordType + ",Field:" + fieldIdx + "/" + (fieldCount - 1) + "," + fieldType.getName());
            }
            int repCount = record.getFieldRepetitionCount(fieldIdx);
            if (repCount > 0) {
                if (loggingEnabled) {
                    logger.info("SER:Writing value for field:" + fieldType.getName());
                }
                oos.writeObject(Boolean.TRUE);
                writeField(fieldType, fieldIdx, record);
            } else {
                if (loggingEnabled) {
                    logger.info("SER:No value for field:" + fieldType.getName());
                }
                oos.writeObject(Boolean.FALSE);
            }
        }
        if (loggingEnabled) {
            logger.info("SER:" + recordType + " DUMP START ####");
            logger.info(record.toString());
            logger.info("SER:" + recordType + " DUMP END ####");
        }
    }

    private void writeArrayOfRecords(int fieldIdx, Group group) throws IOException {
        int fieldRepetitionCount = group.getFieldRepetitionCount(fieldIdx);
        if (loggingEnabled) {
            logger.info("SER:Writing array of records with size:" + fieldRepetitionCount + ",group:" + group.toString());
        }
        oos.writeObject(fieldRepetitionCount);
        for (int j = 0; j < fieldRepetitionCount; j++) {
            Group subGroup = group.getGroup(fieldIdx, j);
            writeRecord(subGroup);
        }
    }

    private void writeArrayOfPrimitives(int fieldIdx, Group group) throws IOException {
        int fieldRepetitionCount = group.getFieldRepetitionCount(fieldIdx);
        if (loggingEnabled) {
            logger.info("SER:Writing array of primitives with size:" + fieldRepetitionCount + ",group:" + group.toString());
        }
        oos.writeObject(fieldRepetitionCount);
        GroupType groupType = group.getType();
        Type type = groupType.getType(0);
        for (int k = 0; k < fieldRepetitionCount; k++) {
            writePrimitive(group, type, 0, k);
        }
    }

    private void writePrimitive(Group record, Type type, int fieldIndex, int index) throws IOException {
        PrimitiveTypeName fieldType = type.asPrimitiveType().getPrimitiveTypeName();
        switch (fieldType) {
            case INT32: {
                int value = record.getInteger(fieldIndex, index);
                if (loggingEnabled) {
                    logger.info("SER:Writing primitive integer:" + value);
                }
                oos.writeObject(value);
                break;
            }
            case INT64: {
                long value = record.getLong(fieldIndex, index);
                if (loggingEnabled) {
                    logger.info("SER:Writing primitive long:" + value);
                }
                oos.writeObject(value);
                break;
            }
            case BINARY: {
                String value = record.getString(fieldIndex, index);
                if (loggingEnabled) {
                    logger.info("SER:Writing primitive string:" + value);
                }
                oos.writeObject(value);
                break;
            }
            case DOUBLE: {
                double value = record.getDouble(fieldIndex, index);
                if (loggingEnabled) {
                    logger.info("SER:Writing primitive double:" + value);
                }
                oos.writeObject(value);
                break;
            }
            case BOOLEAN: {
                boolean value = record.getBoolean(fieldIndex, index);
                if (loggingEnabled) {
                    logger.info("SER:Writing primitive boolean:" + value);
                }
                oos.writeObject(value);
                break;
            }
            default:
                logger.error("Unknown field type: " + fieldType.name());
                break;
        }
    }

    @Override
    public void close() throws IOException {
        Closeables.close(oos);
    }
}
