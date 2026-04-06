package io.github.grantchen2003.cdb.chronicle.bootstrap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChronicleSeqNumBootstrapperTest {
    private static final String STUB_BOOTSTRAP_SERVERS = "stub:0000";
    private static final long TEST_TIMEOUT_MS = 5_000;

    private final ChronicleSeqNumBootstrapper bootstrapper =
            new ChronicleSeqNumBootstrapper(STUB_BOOTSTRAP_SERVERS);

    @Test
    void testLoadChronicleIdSeqNums_emptyClusterReturnsEmptyMap() {
        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of(),
                Map.of(),
                List.of()
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertTrue(result.isEmpty());
    }

    @Test
    void testLoadChronicleIdSeqNums_skipsKafkaInternalTopics() {
        final TopicPartition internalTp = new TopicPartition("__consumer_offsets", 0);

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of("__consumer_offsets", List.of(new PartitionInfo("__consumer_offsets", 0, null, null, null))),
                Map.of(internalTp, 10L),
                List.of()
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertTrue(result.isEmpty(), "Internal Kafka topics should be ignored");
    }

    @Test
    void testLoadChronicleIdSeqNums_singleChronicle() {
        final String chronicleId = "chronicle1";
        final TopicPartition tp = new TopicPartition(chronicleId, 0);

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of(chronicleId, List.of(new PartitionInfo(chronicleId, 0, null, null, null))),
                Map.of(tp, 5L),
                List.of(new ConsumerRecords<>(Map.of(tp, List.of(
                        new ConsumerRecord<>(chronicleId, 0, 4L, "4", "tx")
                ))))
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertEquals(1, result.size());
        assertEquals(4L, result.get(chronicleId));
    }

    @Test
    void testLoadChronicleIdSeqNums_multipleChronicles() {
        final String chronicleId1 = "chronicle1";
        final String chronicleId2 = "chronicle2";
        final TopicPartition tp1 = new TopicPartition(chronicleId1, 0);
        final TopicPartition tp2 = new TopicPartition(chronicleId2, 0);

        final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
        metadata.put(chronicleId1, List.of(new PartitionInfo(chronicleId1, 0, null, null, null)));
        metadata.put(chronicleId2, List.of(new PartitionInfo(chronicleId2, 0, null, null, null)));

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                metadata,
                Map.of(tp1, 12L, tp2, 6L),
                List.of(
                        new ConsumerRecords<>(Map.of(tp1, List.of(new ConsumerRecord<>(chronicleId1, 0, 11L, "11", "tx")))),
                        new ConsumerRecords<>(Map.of(tp2, List.of(new ConsumerRecord<>(chronicleId2, 0, 5L, "5", "tx"))))
                )
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertEquals(2, result.size());
        assertEquals(11L, result.get(chronicleId1));
        assertEquals(5L, result.get(chronicleId2));
    }

    @Test
    void testLoadChronicleIdSeqNums_takesMaxSeqNumAcrossMultipleRecordsInSamePartition() {
        final String chronicleId = "chronicle1";
        final TopicPartition tp = new TopicPartition(chronicleId, 0);

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of(chronicleId, List.of(new PartitionInfo(chronicleId, 0, null, null, null))),
                Map.of(tp, 10L),
                List.of(new ConsumerRecords<>(Map.of(tp, List.of(
                        new ConsumerRecord<>(chronicleId, 0, 9L, "9", "tx"),
                        new ConsumerRecord<>(chronicleId, 0, 7L, "7", "tx")
                ))))
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertEquals(9L, result.get(chronicleId), "Should keep the max seqNum seen");
    }

    @Test
    void testLoadChronicleIdSeqNums_emptyPartitionsAreSkipped() {
        final String chronicleId = "chronicle1";
        final TopicPartition tp = new TopicPartition(chronicleId, 0);

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of(chronicleId, List.of(new PartitionInfo(chronicleId, 0, null, null, null))),
                Map.of(tp, 0L), // empty partition, high watermark = 0
                List.of()
        );

        final Map<String, Long> result = bootstrapper.loadChronicleIdSeqNums(stub, TEST_TIMEOUT_MS);

        assertTrue(result.isEmpty(), "Empty partitions should not block and should contribute nothing");
    }

    @Test
    void testLoadChronicleIdSeqNums_throwsWhenBootstrapTimesOut() {
        final String chronicleId = "chronicle1";
        final TopicPartition tp = new TopicPartition(chronicleId, 0);

        final KafkaConsumerAdapterStub stub = new KafkaConsumerAdapterStub(
                Map.of(chronicleId, List.of(new PartitionInfo(chronicleId, 0, null, null, null))),
                Map.of(tp, 5L),
                List.of() // no records ever returned
        );

        final RuntimeException ex = assertThrows(RuntimeException.class, () ->
                bootstrapper.loadChronicleIdSeqNums(stub, 100)
        );

        assertTrue(ex.getMessage().contains("Bootstrap timed out"));
        assertTrue(ex.getMessage().contains("0 of 1 non-empty partitions"));
    }
}