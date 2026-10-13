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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.processors.producer.wrapper.InjectMetadataRecord;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FlowFile content for each processing strategy, and each record output strategy, when one batch contains a
 * zero-length value, a null tombstone, and a non-empty value.
 */
class ConsumeKafkaMessageValueIT extends AbstractConsumeKafkaIT {

    private static final String NON_ZERO_VALUE = "{\"id\":1,\"name\":\"A\"}";

    private static final byte[] NON_ZERO_BYTES = NON_ZERO_VALUE.getBytes(StandardCharsets.UTF_8);

    private static final byte[] EMPTY_BYTES = new byte[0];

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
    void testFlowFile() throws ExecutionException, InterruptedException {
        final String topic = configure(ProcessingStrategy.FLOW_FILE);
        runner.run(1, false, true);
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size() >= 3, POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(3, flowFiles.size());

        final MockFlowFile emptyFlowFile = flowFiles.get(0);
        emptyFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        emptyFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        emptyFlowFile.assertContentEquals("");
        emptyFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());

        final MockFlowFile nullFlowFile = flowFiles.get(1);
        nullFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        nullFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "1");
        nullFlowFile.assertContentEquals("");
        nullFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());

        final MockFlowFile valueFlowFile = flowFiles.get(2);
        valueFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        valueFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "2");
        valueFlowFile.assertContentEquals(NON_ZERO_VALUE);
        valueFlowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
    }

    @Test
    void testDemarcator() throws ExecutionException, InterruptedException {
        final String topic = configure(ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "3");
        flowFile.assertContentEquals(NON_ZERO_VALUE);
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
    }

    @Test
    void testDemarcatorNull() throws ExecutionException, InterruptedException {
        final String topic = configure(ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceValue(topic, null);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertContentEquals("");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());
    }

    @Test
    void testDemarcatorEmpty() throws ExecutionException, InterruptedException {
        final String topic = configure(ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceValue(topic, EMPTY_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertContentEquals("");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());
    }

    @Test
    void testRecordUseValue() throws ExecutionException, InterruptedException, IOException {
        final String topic = configureRecord(OutputStrategy.USE_VALUE);
        runner.run(1, false, true);
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

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
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

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
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

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
        produceValue(topic, EMPTY_BYTES);
        produceValue(topic, null);
        produceValue(topic, NON_ZERO_BYTES);

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

    private String configureRecord(final OutputStrategy outputStrategy) {
        final String topic = configure(ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy);
        return topic;
    }

    private String configure(final ProcessingStrategy processingStrategy) {
        final String topic = UUID.randomUUID().toString();
        runner.setProperty(ConsumeKafka.GROUP_ID, topic);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, processingStrategy);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        return topic;
    }

    private void produceValue(final String topic, final byte[] value) throws ExecutionException, InterruptedException {
        final Properties properties = getKafkaProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, FIRST_PARTITION, MESSAGE_KEY, value);
            final RecordMetadata metadata = producer.send(record).get();
            assertEquals(topic, metadata.topic());
            assertTrue(metadata.hasOffset());
        }
    }
}
