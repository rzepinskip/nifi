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
package org.apache.nifi.kafka.processors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.processors.producer.wrapper.InjectMetadataRecord;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * RECORD output for a batch that contains a non-null empty value, a null tombstone, and a real value.
 * A null tombstone is omitted from success and parse.failure. A JSON reader emits only the real value.
 * An Avro reader routes the non-null empty value to parse.failure and writes the real value to success.
 */
class ConsumeKafkaRecordTombstoneIT extends AbstractConsumeKafkaIT {

    private static final String NON_ZERO_VALUE = "{\"id\":1,\"name\":\"A\"}";

    private static final byte[] NON_ZERO_BYTES = NON_ZERO_VALUE.getBytes(StandardCharsets.UTF_8);

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "message",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
            """;

    private static final String MESSAGE_KEY = "key";

    private static final int FIRST_PARTITION = 0;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addRecordReaderService(runner);
        addRecordWriterService(runner);
    }

    @Test
    void testRecordUseValue() throws ExecutionException, InterruptedException, IOException {
        final String topic = configureRecord(OutputStrategy.USE_VALUE);
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testRecordUseWrapper() throws ExecutionException, InterruptedException, IOException {
        final String topic = configureRecord(OutputStrategy.USE_WRAPPER);
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        final ObjectNode value = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.VALUE));
        assertEquals(1, value.get("id").asInt());
        assertEquals("A", value.get("name").asText());
        final ObjectNode metadata = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.METADATA));
        assertEquals(topic, metadata.get(WrapperRecord.TOPIC).asText());
        assertEquals(2L, metadata.get(WrapperRecord.OFFSET).asLong());
    }

    @Test
    void testRecordInjectMetadata() throws ExecutionException, InterruptedException, IOException {
        final String topic = configureRecord(OutputStrategy.INJECT_METADATA);
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
        final ObjectNode metadata = assertInstanceOf(ObjectNode.class, record.get(InjectMetadataRecord.METADATA));
        assertEquals(topic, metadata.get(InjectMetadataRecord.TOPIC).asText());
        assertEquals(2L, metadata.get(InjectMetadataRecord.OFFSET).asLong());
    }

    @Test
    void testRecordInjectOffset() throws ExecutionException, InterruptedException, IOException {
        final String topic = configureRecord(OutputStrategy.INJECT_OFFSET);
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
        assertEquals(2L, record.get("kafkaOffset").asLong());
    }

    @Test
    void testRecordAvroUseValue() throws ExecutionException, InterruptedException, IOException, InitializationException {
        final String topic = configureRecord(OutputStrategy.USE_VALUE);
        useAvroReader();
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, avroRecord());

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && !testRunner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, failures.size());

        final MockFlowFile emptyFailure = failures.getFirst();
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        emptyFailure.assertContentEquals("");
        emptyFailure.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testRecordAvroUseWrapper() throws ExecutionException, InterruptedException, IOException, InitializationException {
        final String topic = configureRecord(OutputStrategy.USE_WRAPPER);
        useAvroReader();
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, avroRecord());

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && !testRunner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, failures.size());

        final MockFlowFile emptyFailure = failures.getFirst();
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        emptyFailure.assertContentEquals("");
        emptyFailure.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        final ObjectNode value = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.VALUE));
        assertEquals(1, value.get("id").asInt());
        assertEquals("A", value.get("name").asText());
        final ObjectNode metadata = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.METADATA));
        assertEquals(topic, metadata.get(WrapperRecord.TOPIC).asText());
        assertEquals(2L, metadata.get(WrapperRecord.OFFSET).asLong());
    }

    @Test
    void testRecordAvroInjectMetadata() throws ExecutionException, InterruptedException, IOException, InitializationException {
        final String topic = configureRecord(OutputStrategy.INJECT_METADATA);
        useAvroReader();
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, avroRecord());

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && !testRunner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, failures.size());

        final MockFlowFile emptyFailure = failures.getFirst();
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        emptyFailure.assertContentEquals("");
        emptyFailure.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
        final ObjectNode metadata = assertInstanceOf(ObjectNode.class, record.get(InjectMetadataRecord.METADATA));
        assertEquals(topic, metadata.get(InjectMetadataRecord.TOPIC).asText());
        assertEquals(2L, metadata.get(InjectMetadataRecord.OFFSET).asLong());
    }

    @Test
    void testRecordAvroInjectOffset() throws ExecutionException, InterruptedException, IOException, InitializationException {
        final String topic = configureRecord(OutputStrategy.INJECT_OFFSET);
        useAvroReader();
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, avroRecord());

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && !testRunner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, failures.size());

        final MockFlowFile emptyFailure = failures.getFirst();
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        emptyFailure.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        emptyFailure.assertContentEquals("");
        emptyFailure.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals(1, record.get("id").asInt());
        assertEquals("A", record.get("name").asText());
        assertEquals(2L, record.get("kafkaOffset").asLong());
    }

    private String configureRecord(final OutputStrategy outputStrategy) {
        final String topic = UUID.randomUUID().toString();
        runner.setProperty(ConsumeKafka.GROUP_ID, topic);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy);
        return topic;
    }

    private void useAvroReader() throws InitializationException {
        final String readerId = "Avro Reader";
        final AvroReader reader = new AvroReader();
        runner.addControllerService(readerId, reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema");
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeKafka.RECORD_READER, readerId);
    }

    private byte[] avroRecord() throws IOException {
        final Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        final GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("name", "A");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, outputStream);
            writer.append(record);
        }

        return outputStream.toByteArray();
    }
}
