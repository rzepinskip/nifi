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

import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.kafka.processors.consumer.KafkaMetricName;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.OffsetSummary;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.kafka.processors.ConsumeKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.GROUP_ID;
import static org.apache.nifi.kafka.processors.ConsumeKafka.TOPICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsumeKafkaTest {

    private static final String TEST_TOPIC_NAME = "NiFi-Kafka-Events";

    private static final int FIRST_PARTITION = 0;

    private static final int SECOND_PARTITION = 1;

    private static final byte[] FIRST_RECORD_KEY = "k".getBytes(StandardCharsets.UTF_8);

    private static final byte[] FIRST_RECORD_VALUE = "alpha".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SECOND_RECORD_VALUE = "beta".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SECOND_RECORD_KEY = "k2".getBytes(StandardCharsets.UTF_8);

    private static final long FIRST_RECORD_BYTES = FIRST_RECORD_KEY.length + FIRST_RECORD_VALUE.length;

    private static final long SECOND_RECORD_BYTES = SECOND_RECORD_KEY.length + SECOND_RECORD_VALUE.length;

    private static final String RECORD_DEMARCATOR = "\n";

    private static final long FIRST_PARTITION_LAG = 4L;

    private static final long SECOND_PARTITION_LAG = 9L;

    private static final String BROKER_URI = "kafka://localhost";

    private static final String MAX_UNCOMMITTED_TIME = "250 ms";

    private static final String DYNAMIC_PROPERTY_KEY_PUBLISH = "delivery.timeout.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_PUBLISH = "60000";
    private static final String DYNAMIC_PROPERTY_KEY_CONSUME = "fetch.max.wait.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_CONSUME = "1000";

    private static final String SERVICE_ID = KafkaConnectionService.class.getSimpleName();

    private static final String READER_ID = "record-reader";

    private static final String WRITER_ID = "record-writer";

    private static final String CONSUMER_GROUP_ID = ConsumeKafkaTest.class.getSimpleName();

    @Mock
    KafkaConnectionService kafkaConnectionService;

    @Mock
    KafkaConsumerService kafkaConsumerService;

    private TestRunner runner;

    private ConsumeKafka processor;

    @BeforeEach
    public void setRunner() {
        processor = new ConsumeKafka();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProperties() throws InitializationException {
        runner.assertNotValid();

        setConnectionService();
        runner.assertNotValid();

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.assertValid();
    }

    @Test
    public void testVerifySuccessful() throws InitializationException {
        final PartitionState firstPartitionState = new PartitionState(TEST_TOPIC_NAME, FIRST_PARTITION);
        final List<PartitionState> partitionStates = Collections.singletonList(firstPartitionState);
        when(kafkaConsumerService.getPartitionStates()).thenReturn(partitionStates);
        setConnectionService();
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        final List<ConfigVerificationResult> successResults = results.stream()
            .filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL)
            .toList();
        assertEquals(1, successResults.size());

        final boolean anyFailures = results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED);
        assertFalse(anyFailures, "At least one verification result was a failure: " + results);

        final ConfigVerificationResult firstResult = successResults.getFirst();
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testVerifyFailed() throws InitializationException {
        when(kafkaConsumerService.getPartitionStates()).thenThrow(new IllegalStateException());
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        setConnectionService();

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(2, results.size());

        final List<ConfigVerificationResult> failedResults = results.stream()
                .filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.FAILED)
                .toList();
        assertEquals(1, failedResults.size());

        final ConfigVerificationResult firstResult = failedResults.getFirst();
        assertEquals(ConfigVerificationResult.Outcome.FAILED, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testDynamicProperties() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.setProperty(kafkaConnectionService, DYNAMIC_PROPERTY_KEY_PUBLISH, DYNAMIC_PROPERTY_VALUE_PUBLISH);
        runner.setProperty(kafkaConnectionService, DYNAMIC_PROPERTY_KEY_CONSUME, DYNAMIC_PROPERTY_VALUE_CONSUME);
        runner.enableControllerService(kafkaConnectionService);
    }

    @Test
    public void testGetBacklogQueriesAdminClientRegardlessOfPoolState() throws Exception {
        // getBacklog always queries committed/end offsets via the connection service's Admin
        // client. The broker is the source of truth for the consumer group's committed offsets
        // across every assigned partition, so the returned lag covers the entire group rather
        // than only the partitions owned by whichever consumer instances happen to be pooled or
        // checked out at this moment. Per-partition undercounts cannot occur because the consumer
        // pool is not consulted on this path.
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(42L);

        // Reflectively place one consumer in the pool and mark one additional consumer as checked
        // out by a running onTrigger task. The Admin-client answer must be returned even when the
        // pool contains a consumer that could be inspected directly.
        final Field consumerServicesField = ConsumeKafka.class.getDeclaredField("consumerServices");
        consumerServicesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        final Queue<KafkaConsumerService> consumerServices = (Queue<KafkaConsumerService>) consumerServicesField.get(processor);
        consumerServices.offer(kafkaConsumerService);

        final Field activeConsumerCountField = ConsumeKafka.class.getDeclaredField("activeConsumerCount");
        activeConsumerCountField.setAccessible(true);
        final AtomicInteger activeConsumerCount = (AtomicInteger) activeConsumerCountField.get(processor);
        activeConsumerCount.set(1);

        final Optional<Backlog> backlog = processor.getBacklog(runner.getProcessContext());

        assertTrue(backlog.isPresent());
        assertEquals(OptionalLong.of(42L), backlog.get().getRecordCount());
        assertEquals(Backlog.Precision.EXACT, backlog.get().getPrecision());
        assertTrue(backlog.get().getLastCaughtUp().isEmpty());
        // The pooled consumer must not be interrogated for partition state or lag; that path is
        // bypassed entirely by the Admin-client design.
        verify(kafkaConsumerService, never()).getPartitionStates();
        verify(kafkaConsumerService, never()).currentLag(any());
    }

    @Test
    public void testGetBacklogPopulatesZeroRecordsOnCaughtUp() throws Exception {
        // A caught-up consumer group has an exactly-known lag of zero. The returned Backlog must
        // populate the records dimension with that zero alongside lastCaughtUp, so that clients
        // can render "0 records" rather than treating the record count as unknown.
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(0L);

        final Optional<Backlog> backlog = processor.getBacklog(runner.getProcessContext());

        assertTrue(backlog.isPresent());
        assertEquals(OptionalLong.of(0L), backlog.get().getRecordCount());
        assertEquals(Backlog.Precision.EXACT, backlog.get().getPrecision());
        assertTrue(backlog.get().getLastCaughtUp().isPresent());
    }

    @Test
    public void testOnStoppedPreservesLastCaughtUp() throws Exception {
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(0L).thenReturn(5L);

        final Optional<Backlog> initialBacklog = processor.getBacklog(runner.getProcessContext());
        assertTrue(initialBacklog.isPresent());
        final Instant rememberedFirst = initialBacklog.get().getLastCaughtUp().orElse(null);
        assertNotNull(rememberedFirst);

        processor.onStopped();

        final Optional<Backlog> afterStop = processor.getBacklog(runner.getProcessContext());
        assertTrue(afterStop.isPresent());
        // After the processor is stopped while non-zero lag exists, the remembered caught-up
        // timestamp from the prior caught-up observation must still be surfaced — onStopped() must
        // not clear lastCaughtUpTimestamp.
        assertEquals(Optional.of(rememberedFirst), afterStop.get().getLastCaughtUp());
        assertEquals(OptionalLong.of(5L), afterStop.get().getRecordCount());
    }

    @Test
    public void testOnTriggerRecordsBytesAndLagMetrics() throws InitializationException {
        setConnectionService();
        configureProcessor();
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final ByteRecord firstRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, List.of(), FIRST_RECORD_KEY, FIRST_RECORD_VALUE, 1);
        final ByteRecord secondRecord = new ByteRecord(TEST_TOPIC_NAME, SECOND_PARTITION, 0, 0L, List.of(), null, SECOND_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(firstRecord, secondRecord)).thenReturn(List.of());

        final TopicPartitionSummary firstPartition = new TopicPartitionSummary(TEST_TOPIC_NAME, FIRST_PARTITION);
        final TopicPartitionSummary secondPartition = new TopicPartitionSummary(TEST_TOPIC_NAME, SECOND_PARTITION);
        when(kafkaConsumerService.currentLag(firstPartition)).thenReturn(OptionalLong.of(FIRST_PARTITION_LAG));
        when(kafkaConsumerService.currentLag(secondPartition)).thenReturn(OptionalLong.of(SECOND_PARTITION_LAG));

        runner.run();

        final Map<String, String> firstAttributes = topicPartitionAttributes(TEST_TOPIC_NAME, FIRST_PARTITION);
        final Map<String, String> secondAttributes = topicPartitionAttributes(TEST_TOPIC_NAME, SECOND_PARTITION);

        assertEquals(1L, runner.getCounterValue(KafkaMetricName.RECORDS_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(1L, runner.getCounterValue(KafkaMetricName.RECORDS_CONSUMED.getMetricName(), secondAttributes));
        assertEquals(FIRST_RECORD_BYTES, runner.getCounterValue(KafkaMetricName.BYTES_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(SECOND_RECORD_VALUE.length, runner.getCounterValue(KafkaMetricName.BYTES_CONSUMED.getMetricName(), secondAttributes));
        assertEquals(2L, runner.getCounterValue("Records Received from " + TEST_TOPIC_NAME));

        assertFalse(runner.getGaugeValues(KafkaMetricName.RECORDS_CONSUMER_LAG.getMetricName(), firstAttributes).isEmpty());
        assertFalse(runner.getGaugeValues(KafkaMetricName.RECORDS_CONSUMER_LAG.getMetricName(), secondAttributes).isEmpty());
        assertTrue(runner.getGaugeValues(KafkaMetricName.RECORDS_CONSUMER_LAG.getMetricName(), firstAttributes).stream().allMatch(value -> value == FIRST_PARTITION_LAG));
        assertTrue(runner.getGaugeValues(KafkaMetricName.RECORDS_CONSUMER_LAG.getMetricName(), secondAttributes).stream().allMatch(value -> value == SECOND_PARTITION_LAG));
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 2);
    }

    @Test
    public void testOnTriggerEmptyPollDoesNotRecordLag() throws InitializationException {
        setConnectionService();
        configureProcessor();
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of());

        runner.run();

        verify(kafkaConsumerService, never()).currentLag(any());
        final Map<String, String> firstAttributes = topicPartitionAttributes(TEST_TOPIC_NAME, FIRST_PARTITION);
        assertTrue(runner.getGaugeValues(KafkaMetricName.RECORDS_CONSUMER_LAG.getMetricName(), firstAttributes).isEmpty());
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        assertNull(runner.getCounterValue(KafkaMetricName.RECORDS_CONSUMED.getMetricName(), firstAttributes));
    }

    @Test
    public void testOnTriggerEmitsConsumptionWhenPollFailsAfterRecords() throws InitializationException {
        setConnectionService();
        configureProcessor();
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final ByteRecord firstRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, List.of(), FIRST_RECORD_KEY, FIRST_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(firstRecord)).thenThrow(new RuntimeException("poll failed"));

        runner.run();

        final Map<String, String> firstAttributes = topicPartitionAttributes(TEST_TOPIC_NAME, FIRST_PARTITION);
        assertEquals(1L, runner.getCounterValue(KafkaMetricName.RECORDS_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(FIRST_RECORD_BYTES, runner.getCounterValue(KafkaMetricName.BYTES_CONSUMED.getMetricName(), firstAttributes));
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
    }

    @Test
    public void testOnTriggerDemarcatorRecordsOriginalKafkaBytes() throws InitializationException {
        setConnectionService();
        configureProcessor();
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, RECORD_DEMARCATOR);
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final ByteRecord firstRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, new ArrayList<>(), FIRST_RECORD_KEY, FIRST_RECORD_VALUE, 1);
        final ByteRecord secondRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 1, 0L, new ArrayList<>(), SECOND_RECORD_KEY, SECOND_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(firstRecord, secondRecord)).thenReturn(List.of());

        final TopicPartitionSummary firstPartition = new TopicPartitionSummary(TEST_TOPIC_NAME, FIRST_PARTITION);
        when(kafkaConsumerService.currentLag(firstPartition)).thenReturn(OptionalLong.of(FIRST_PARTITION_LAG));

        runner.run();

        final Map<String, String> firstAttributes = topicPartitionAttributes(TEST_TOPIC_NAME, FIRST_PARTITION);
        final long originalKafkaBytes = FIRST_RECORD_BYTES + SECOND_RECORD_BYTES;
        final long bundledFlowFileBytes = FIRST_RECORD_VALUE.length
                + RECORD_DEMARCATOR.getBytes(StandardCharsets.UTF_8).length
                + SECOND_RECORD_VALUE.length;

        assertEquals(2L, runner.getCounterValue(KafkaMetricName.RECORDS_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(originalKafkaBytes, runner.getCounterValue(KafkaMetricName.BYTES_CONSUMED.getMetricName(), firstAttributes));
        assertNotEquals(bundledFlowFileBytes, originalKafkaBytes);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
    }

    @Test
    public void testOnTriggerDemarcatorSkipsTombstoneAndKeepsEmptyValue() throws Exception {
        setConnectionService();
        configureProcessor();
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, RECORD_DEMARCATOR);
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final ByteRecord emptyRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, new ArrayList<>(), null, new byte[0], 1);
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 1, 0L, new ArrayList<>(), null, new byte[0], 1, true);
        final ByteRecord valueRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 2, 0L, new ArrayList<>(), null, FIRST_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(emptyRecord, tombstone, valueRecord)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertContentEquals(FIRST_RECORD_VALUE);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "2");
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
        assertEquals(3L, runner.getCounterValue("Records Received from " + TEST_TOPIC_NAME));
        assertEquals(2L, committedOffset());
    }

    @Test
    public void testOnTriggerDemarcatorCommitsSkippedTombstoneOffset() throws Exception {
        setConnectionService();
        configureProcessor();
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, RECORD_DEMARCATOR);
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final long tombstoneOffset = 4L;
        final ByteRecord valueRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, new ArrayList<>(), null, FIRST_RECORD_VALUE, 1);
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, tombstoneOffset, 0L, new ArrayList<>(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(valueRecord, tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertContentEquals(FIRST_RECORD_VALUE);
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        assertEquals(tombstoneOffset, committedOffset());
    }

    @Test
    public void testOnTriggerDemarcatorSkipsTombstoneOnlyPoll() throws InitializationException {
        setConnectionService();
        configureProcessor();
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, RECORD_DEMARCATOR);
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, new ArrayList<>(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        assertEquals(1L, runner.getCounterValue("Records Received from " + TEST_TOPIC_NAME));
        assertEquals(0L, committedOffset());
    }

    @Test
    public void testVerifySkipsTombstoneAndParsesFollowingRecord() throws Exception {
        final RecordReaderFactory readerFactory = recordReaderFactory();
        final RecordReader recordReader = mock(RecordReader.class);
        when(readerFactory.createRecordReader(any(), any(), anyLong(), any())).thenAnswer(invocation -> {
            final long length = invocation.getArgument(2);
            if (length == 0L) {
                throw new IOException("empty value");
            }
            return recordReader;
        });

        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, List.of(), null, new byte[0], 1, true);
        final ByteRecord dataRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 1L, 0L, List.of(), null, FIRST_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone, dataRecord));

        final ConfigVerificationResult parseResult = parseResult(verifyRecords());
        assertEquals(Outcome.SUCCESSFUL, parseResult.getOutcome());
        assertEquals("Successfully parsed 2 records (skipped 1 tombstones)", parseResult.getExplanation());
    }

    @Test
    public void testVerifySucceedsWhenSampleContainsOnlyTombstones() throws Exception {
        final RecordReaderFactory readerFactory = recordReaderFactory();
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, List.of(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone));

        final ConfigVerificationResult parseResult = parseResult(verifyRecords());
        assertEquals(Outcome.SUCCESSFUL, parseResult.getOutcome());
        assertEquals("Successfully parsed 1 records (skipped 1 tombstones)", parseResult.getExplanation());
        verify(readerFactory, never()).createRecordReader(any(), any(), anyLong(), any());
    }

    @Test
    public void testVerifyPreserveTombstonesReportsWrittenNullValues() throws Exception {
        final RecordReaderFactory readerFactory = recordReaderFactory();
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, List.of(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone));

        final ConfigVerificationResult parseResult = parseResult(verifyRecords());
        assertEquals(Outcome.SUCCESSFUL, parseResult.getOutcome());
        assertEquals("Did not pass 1 null values to the Record Reader; Preserve Tombstones writes those records with kafka.tombstone in the record metadata",
                parseResult.getExplanation());
        verify(readerFactory, never()).createRecordReader(any(), any(), anyLong(), any());
    }

    @Test
    public void testVerifyReportsDataRecordWhenTombstonePrecedesParseFailure() throws Exception {
        final RecordReaderFactory readerFactory = recordReaderFactory();
        when(readerFactory.createRecordReader(any(), any(), anyLong(), any())).thenThrow(new IOException("empty value"));

        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, List.of(), null, new byte[0], 1, true);
        final ByteRecord dataRecord = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 1L, 0L, List.of(), null, FIRST_RECORD_VALUE, 1);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone, dataRecord));

        final ConfigVerificationResult parseResult = parseResult(verifyRecords());
        assertEquals(Outcome.FAILED, parseResult.getOutcome());
        assertEquals("Failed to parse Record number 2: java.io.IOException: empty value", parseResult.getExplanation());
        verify(readerFactory).createRecordReader(any(), any(), anyLong(), any());
    }

    @Test
    public void testPreserveTombstonesValidForWrapperStrategies() throws InitializationException {
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        enableRecordServices(new MockRecordParser(), writer);
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");
        runner.assertValid();

        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.INJECT_METADATA);
        runner.assertValid();
    }

    @Test
    public void testPreserveTombstoneUseWrapperTransfersToSuccess() throws Exception {
        assertPreservedTombstone(OutputStrategy.USE_WRAPPER);
    }

    @Test
    public void testPreserveTombstoneInjectMetadataTransfersToSuccess() throws Exception {
        assertPreservedTombstone(OutputStrategy.INJECT_METADATA);
    }

    @Test
    public void testPreserveTombstoneUseWrapperCommitsOffsetPastPrecedingValue() throws Exception {
        assertPreservedTombstoneFollowsValue(OutputStrategy.USE_WRAPPER);
    }

    @Test
    public void testPreserveTombstoneInjectMetadataCommitsOffsetPastPrecedingValue() throws Exception {
        assertPreservedTombstoneFollowsValue(OutputStrategy.INJECT_METADATA);
    }

    @Test
    public void testPreserveTombstonesIgnoredForDemarcator() throws InitializationException {
        setConnectionService();
        configureProcessor();
        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, RECORD_DEMARCATOR);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");

        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, new ArrayList<>(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        assertEquals(1L, runner.getCounterValue("Records Received from " + TEST_TOPIC_NAME));
    }

    @Test
    public void testPreserveTombstonesIgnoredForUseValue() throws Exception {
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn(READER_ID);
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        enableRecordServices(readerFactory, writer);
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");

        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0, 0L, List.of(), null, new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        verify(readerFactory, never()).createRecordReader(any(), any(), anyLong(), any());
    }

    private void assertPreservedTombstone(final OutputStrategy outputStrategy) throws InitializationException {
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        enableRecordServices(new MockRecordParser(), writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, "output-oneline");
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");
        runner.assertValid();

        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 1L, 0L, List.of(),
                "acct".getBytes(StandardCharsets.UTF_8), new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
        flowFile.assertAttributeEquals("record.count", "1");
        final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(content.contains("\"kafka.tombstone\":true"));
    }

    private void assertPreservedTombstoneFollowsValue(final OutputStrategy outputStrategy) throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addRecord("alpha");
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        enableRecordServices(parser, writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, "output-oneline");
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy);
        runner.setProperty(ConsumeKafka.PRESERVE_TOMBSTONES, "true");
        runner.assertValid();

        when(kafkaConnectionService.getBrokerUri()).thenReturn(BROKER_URI);
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        final ByteRecord value = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, List.of(), null, FIRST_RECORD_VALUE, 1);
        final ByteRecord tombstone = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 2L, 0L, List.of(),
                "acct".getBytes(StandardCharsets.UTF_8), new byte[0], 1, true);
        when(kafkaConsumerService.poll(any(Duration.class))).thenReturn(List.of(value, tombstone)).thenReturn(List.of());
        when(kafkaConsumerService.currentLag(any())).thenReturn(OptionalLong.empty());

        runner.run();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        final MockFlowFile valueFlowFile = flowFiles.stream()
                .filter(flowFile -> new String(flowFile.toByteArray(), StandardCharsets.UTF_8).contains("alpha"))
                .findFirst()
                .orElseThrow();
        final MockFlowFile tombstoneFlowFile = flowFiles.stream()
                .filter(flowFile -> new String(flowFile.toByteArray(), StandardCharsets.UTF_8).contains("\"kafka.tombstone\":true"))
                .findFirst()
                .orElseThrow();
        valueFlowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
        valueFlowFile.assertAttributeEquals("record.count", "1");
        tombstoneFlowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
        tombstoneFlowFile.assertAttributeEquals("record.count", "1");
        assertEquals(2L, committedOffset());
    }

    private void enableRecordServices(final RecordReaderFactory reader, final RecordSetWriterFactory writer) throws InitializationException {
        runner.addControllerService(READER_ID, reader);
        runner.enableControllerService(reader);
        runner.addControllerService(WRITER_ID, writer);

        setConnectionService();
        configureProcessor();
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.RECORD_READER, READER_ID);
        runner.setProperty(ConsumeKafka.RECORD_WRITER, WRITER_ID);
    }

    private RecordReaderFactory recordReaderFactory() throws InitializationException {
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn(READER_ID);
        runner.addControllerService(READER_ID, readerFactory);
        runner.enableControllerService(readerFactory);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.RECORD_READER, READER_ID);
        return readerFactory;
    }

    private List<ConfigVerificationResult> verifyRecords() throws InitializationException {
        when(kafkaConsumerService.getPartitionStates()).thenReturn(List.of(new PartitionState(TEST_TOPIC_NAME, FIRST_PARTITION)));
        setConnectionService();
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        return processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
    }

    private static ConfigVerificationResult parseResult(final List<ConfigVerificationResult> results) {
        return results.stream()
                .filter(result -> "Parse Records".equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow();
    }

    private long committedOffset() {
        final ArgumentCaptor<PollingSummary> pollingSummaryCaptor = ArgumentCaptor.forClass(PollingSummary.class);
        verify(kafkaConsumerService).commit(pollingSummaryCaptor.capture());
        final OffsetSummary offsetSummary = pollingSummaryCaptor.getValue().getOffsets().get(new TopicPartitionSummary(TEST_TOPIC_NAME, FIRST_PARTITION));
        assertNotNull(offsetSummary);
        return offsetSummary.getOffset();
    }

    private void configureProcessor() {
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(ConsumeKafka.MAX_UNCOMMITTED_TIME, MAX_UNCOMMITTED_TIME);
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }

    private static Map<String, String> topicPartitionAttributes(final String topic, final int partition) {
        return Map.of(
                KafkaFlowFileAttribute.KAFKA_TOPIC, topic,
                KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(partition)
        );
    }
}
