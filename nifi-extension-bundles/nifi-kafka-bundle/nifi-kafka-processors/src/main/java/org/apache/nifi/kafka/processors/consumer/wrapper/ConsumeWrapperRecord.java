/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.kafka.processors.consumer.wrapper;

import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumeWrapperRecord {

    private static final RecordSchema EMPTY_SCHEMA = new SimpleRecordSchema(List.of());

    private final HeaderValueConverter headerValueConverter;

    public ConsumeWrapperRecord(final HeaderValueConverter headerValueConverter) {
        this.headerValueConverter = headerValueConverter;
    }

    public MapRecord toWrapperRecord(final ByteRecord consumerRecord,
                                     final Record record,
                                     final Tuple<RecordField, Object> tupleKey) {
        return toRecord(consumerRecord, record, tupleKey, false);
    }

    public MapRecord toTombstoneRecord(final ByteRecord consumerRecord, final Tuple<RecordField, Object> tupleKey) {
        return toRecord(consumerRecord, null, tupleKey, true);
    }

    private MapRecord toRecord(final ByteRecord consumerRecord,
                               final Record record,
                               final Tuple<RecordField, Object> tupleKey,
                               final boolean tombstone) {
        final Tuple<RecordField, Object> tupleValue = toWrapperRecordValue(record);
        final Tuple<RecordField, Object> tupleHeaders = toWrapperRecordHeaders(consumerRecord);
        final Tuple<RecordField, Object> tupleMetadata = toWrapperRecordMetadata(consumerRecord, tombstone);
        final RecordSchema rootRecordSchema = tupleKey.getKey() == null
                ? new SimpleRecordSchema(List.of(tupleValue.getKey(), tupleHeaders.getKey(), tupleMetadata.getKey()))
                : new SimpleRecordSchema(List.of(tupleKey.getKey(), tupleValue.getKey(), tupleHeaders.getKey(), tupleMetadata.getKey()));

        final Map<String, Object> recordValues = new HashMap<>();
        if (tupleKey.getKey() != null) {
            recordValues.put(tupleKey.getKey().getFieldName(), tupleKey.getValue());
        }

        recordValues.put(tupleValue.getKey().getFieldName(), tupleValue.getValue());
        recordValues.put(tupleHeaders.getKey().getFieldName(), tupleHeaders.getValue());
        recordValues.put(tupleMetadata.getKey().getFieldName(), tupleMetadata.getValue());
        return new MapRecord(rootRecordSchema, recordValues);
    }

    private Tuple<RecordField, Object> toWrapperRecordValue(final Record record) {
        final RecordSchema recordSchema = (record == null) ? EMPTY_SCHEMA : record.getSchema();
        final RecordField recordField = new RecordField(WrapperRecord.VALUE, RecordFieldType.RECORD.getRecordDataType(recordSchema));
        return new Tuple<>(recordField, record);
    }

    private Tuple<RecordField, Object> toWrapperRecordHeaders(final ByteRecord consumerRecord) {
        final RecordField recordField = new RecordField(WrapperRecord.HEADERS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        final Map<String, String> headers = new HashMap<>();
        for (final RecordHeader header : consumerRecord.getHeaders()) {
            headers.put(header.key(), headerValueConverter.convert(header.value()));
        }
        return new Tuple<>(recordField, headers);
    }

    private static final RecordField FIELD_TOPIC = new RecordField(WrapperRecord.TOPIC, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION = new RecordField(WrapperRecord.PARTITION, RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_OFFSET = new RecordField(WrapperRecord.OFFSET, RecordFieldType.LONG.getDataType());
    private static final RecordField FIELD_TIMESTAMP = new RecordField(WrapperRecord.TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());
    private static final RecordField FIELD_TOMBSTONE = new RecordField(
            KafkaFlowFileAttribute.KAFKA_TOMBSTONE, RecordFieldType.BOOLEAN.getDataType());
    private static final RecordSchema SCHEMA_WRAPPER = new SimpleRecordSchema(List.of(
            FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP));
    private static final RecordSchema SCHEMA_TOMBSTONE = new SimpleRecordSchema(List.of(
            FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP, FIELD_TOMBSTONE));

    private Tuple<RecordField, Object> toWrapperRecordMetadata(final ByteRecord consumerRecord, final boolean tombstone) {
        final RecordSchema schema = tombstone ? SCHEMA_TOMBSTONE : SCHEMA_WRAPPER;
        final RecordField recordField = new RecordField(WrapperRecord.METADATA, RecordFieldType.RECORD.getRecordDataType(schema));
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(WrapperRecord.TOPIC, consumerRecord.getTopic());
        metadata.put(WrapperRecord.PARTITION, consumerRecord.getPartition());
        metadata.put(WrapperRecord.OFFSET, consumerRecord.getOffset());
        metadata.put(WrapperRecord.TIMESTAMP, consumerRecord.getTimestamp());
        if (tombstone) {
            metadata.put(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE);
        }

        final Record record = new MapRecord(schema, metadata);
        return new Tuple<>(recordField, record);
    }
}
