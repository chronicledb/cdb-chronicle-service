package io.github.grantchen2003.cdb.chronicle;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class ChronicleSnBootstrapperTest {

    @Test
    void testLoadCdbIdSeqNums_successfullyRecoversTwoPartitions() {
        final String cdbId1 = "cdb1";
        final String cdbId2 = "cdb2";
        final TopicPartition tp1 = new TopicPartition(cdbId1, 0);
        final TopicPartition tp2 = new TopicPartition(cdbId2, 0);

        try (final MockedConstruction<KafkaConsumer> mocked = mockConstruction(KafkaConsumer.class, (mock, context) -> {
            final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
            metadata.put(cdbId1, List.of(new PartitionInfo(cdbId1, 0, null, null, null)));
            metadata.put(cdbId2, List.of(new PartitionInfo(cdbId2, 0, null, null, null)));
            when(mock.listTopics()).thenReturn(metadata);

            final Map<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(tp1, 12L);
            endOffsets.put(tp2, 6L);
            when(mock.endOffsets(any())).thenReturn(endOffsets);

            final ConsumerRecord<String, String> rec1 = new ConsumerRecord<>(cdbId1, 0, 11L, "11", "v2");
            final ConsumerRecord<String, String> rec2 = new ConsumerRecord<>(cdbId2, 0, 5L, "5", "v3");

            when(mock.poll(any(Duration.class)))
                    .thenReturn(new ConsumerRecords<>(Map.of(tp1, List.of(rec1))))
                    .thenReturn(new ConsumerRecords<>(Map.of(tp2, List.of(rec2))))
                    .thenReturn(ConsumerRecords.empty());
        })) {
            final Map<String, Long> result = ChronicleSnBootstrapper.loadCdbIdSeqNums("localhost:9092");

            assertEquals(2, result.size(), "Should have found 2 distinct cdb_ids");
            assertEquals(11L, result.get(cdbId1), "Should recover 11 for cdb1");
            assertEquals(5L, result.get(cdbId2), "Should recover 5 for cdb2");
            assertEquals(1, mocked.constructed().size(), "KafkaConsumer should be instantiated once");
        }
    }

    @Test
    void testLoadCdbIdSeqNums_throwsWhenBootstrapTimesOut() {
        final String cdbId1 = "cdb1";
        final TopicPartition tp1 = new TopicPartition(cdbId1, 0);

        try (final MockedConstruction<KafkaConsumer> ignored = mockConstruction(KafkaConsumer.class, (mock, context) -> {
            final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
            metadata.put(cdbId1, List.of(new PartitionInfo(cdbId1, 0, null, null, null)));
            when(mock.listTopics()).thenReturn(metadata);

            when(mock.endOffsets(any())).thenReturn(Map.of(tp1, 5L));

            // poll() never returns the partition's last record, simulating a stalled broker
            when(mock.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        })) {
            final RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    ChronicleSnBootstrapper.loadCdbIdSeqNums("localhost:9092", 100)
            );

            assertTrue(ex.getMessage().contains("Bootstrap timed out"));
            assertTrue(ex.getMessage().contains("0 of 1 partitions"));
        }
    }
}