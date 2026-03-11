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
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class ChronicleSnBootstrapperTest {
    @Test
    void testLoadCdbIdSeqNums() {
        final String cdbId1 = "cdb1";
        final String cdbId2 = "cdb2";
        final TopicPartition tp1 = new TopicPartition(cdbId1, 0);
        final TopicPartition tp2 = new TopicPartition(cdbId2, 0);

        try (final MockedConstruction<KafkaConsumer> mocked = mockConstruction(KafkaConsumer.class, (mock, context) -> {
            // 1. Stub listTopics() so the code knows which DBs to process
            final Map<String, List<PartitionInfo>> metadata = new HashMap<>();
            metadata.put(cdbId1, List.of(new PartitionInfo(cdbId1, 0, null, null, null)));
            metadata.put(cdbId2, List.of(new PartitionInfo(cdbId2, 0, null, null, null)));
            when(mock.listTopics()).thenReturn(metadata);

            // 2. Stub endOffsets() to simulate the high watermark
            // For db-1: offset 12 (last record is 11). For db-2: offset 6 (last record is 5).
            final Map<TopicPartition, Long> endOffsets = new HashMap<>();
            endOffsets.put(tp1, 12L);
            endOffsets.put(tp2, 6L);
            when(mock.endOffsets(any())).thenReturn(endOffsets);

            // 3. Stub poll() to return our "Latest" records
            final ConsumerRecord<String, String> rec1 = new ConsumerRecord<>(cdbId1, 0, 11L, "11", "v2");
            final ConsumerRecord<String, String> rec2 = new ConsumerRecord<>(cdbId2, 0, 5L, "5", "v3");

            final ConsumerRecords<String, String> batch1 = new ConsumerRecords<>(Map.of(tp1, List.of(rec1)));
            final ConsumerRecords<String, String> batch2 = new ConsumerRecords<>(Map.of(tp2, List.of(rec2)));

            // Simulate the polling sequence: Return rec1, then rec2, then stop.
            when(mock.poll(any(Duration.class)))
                    .thenReturn(batch1)
                    .thenReturn(batch2)
                    .thenReturn(ConsumerRecords.empty());
        })) {
            // Execute the original method
            final ConcurrentHashMap<String, Long> result =
                    ChronicleSnBootstrapper.loadCdbIdSeqNums("localhost:9092");

            // Verify the results match our seeded mock data
            assertEquals(2, result.size(), "Should have found 2 distinct cdb_ids");
            assertEquals(11L, result.get(cdbId1), "Should recover 11 for db-1");
            assertEquals(5L, result.get(cdbId2), "Should recover 5 for db-2");
            assertEquals(1, mocked.constructed().size(), "KafkaConsumer should be instantiated once");
        }
    }
}