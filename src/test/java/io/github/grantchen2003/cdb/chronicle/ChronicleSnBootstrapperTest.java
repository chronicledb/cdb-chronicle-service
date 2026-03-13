package io.github.grantchen2003.cdb.chronicle;

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

class ChronicleSnBootstrapperTest {
    private static final String STUB_BOOTSTRAP_SERVERS = "stub:0000";
    private final ChronicleSnBootstrapper bootstrapper = new ChronicleSnBootstrapper(STUB_BOOTSTRAP_SERVERS);

    @Test
    void testLoadCdbIdSeqNums_successfullyRecoversTwoPartitions() {
        final String cdbId1 = "cdb1";
        final String cdbId2 = "cdb2";
        final TopicPartition tp1 = new TopicPartition(cdbId1, 0);
        final TopicPartition tp2 = new TopicPartition(cdbId2, 0);

        final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
        metadata.put(cdbId1, List.of(new PartitionInfo(cdbId1, 0, null, null, null)));
        metadata.put(cdbId2, List.of(new PartitionInfo(cdbId2, 0, null, null, null)));

        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(tp1, 12L);
        endOffsets.put(tp2, 6L);

        final ConsumerRecord<String, String> rec1 = new ConsumerRecord<>(cdbId1, 0, 11L, "11", "v2");
        final ConsumerRecord<String, String> rec2 = new ConsumerRecord<>(cdbId2, 0, 5L, "5", "v3");

        final KafkaConsumerStub stub = new KafkaConsumerStub(
                metadata,
                endOffsets,
                List.of(
                        new ConsumerRecords<>(Map.of(tp1, List.of(rec1))),
                        new ConsumerRecords<>(Map.of(tp2, List.of(rec2)))
                )
        );

        final Map<String, Long> result = bootstrapper.loadCdbIdSeqNums(stub, 5_000);

        assertEquals(2, result.size(), "Should have found 2 distinct cdb_ids");
        assertEquals(11L, result.get(cdbId1), "Should recover 11 for cdb1");
        assertEquals(5L, result.get(cdbId2), "Should recover 5 for cdb2");
    }

    @Test
    void testLoadCdbIdSeqNums_throwsWhenBootstrapTimesOut() {
        final String cdbId1 = "cdb1";
        final TopicPartition tp1 = new TopicPartition(cdbId1, 0);

        final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
        metadata.put(cdbId1, List.of(new PartitionInfo(cdbId1, 0, null, null, null)));

        final KafkaConsumerStub stub = new KafkaConsumerStub(
                metadata,
                Map.of(tp1, 5L),
                List.of()
        );

        final RuntimeException ex = assertThrows(RuntimeException.class, () ->
                bootstrapper.loadCdbIdSeqNums(stub, 100)
        );

        assertTrue(ex.getMessage().contains("Bootstrap timed out"));
        assertTrue(ex.getMessage().contains("0 of 1 partitions"));
    }
}