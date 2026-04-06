package io.github.grantchen2003.cdb.chronicle.bootstrap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ChronicleSeqNumBootstrapper {
    private static final long DEFAULT_BOOTSTRAP_TIMEOUT_MS = 30_000;

    private final String bootstrapServers;

    public ChronicleSeqNumBootstrapper(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, Long> loadChronicleIdSeqNums() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            return loadChronicleIdSeqNums(new KafkaConsumerAdapterImpl(consumer), DEFAULT_BOOTSTRAP_TIMEOUT_MS);
        }
    }

    public Map<String, Long> loadChronicleIdSeqNums(KafkaConsumerAdapter adapter, long timeoutMs) {
        final List<TopicPartition> allUserPartitions = getAllUserTopicPartitions(adapter);

        adapter.assign(allUserPartitions);

        final Map<TopicPartition, Long> highWatermarks = adapter.endOffsets(allUserPartitions);
        seekToTailRecords(adapter, allUserPartitions, highWatermarks);

        return collectLatestSeqNums(adapter, allUserPartitions, highWatermarks, timeoutMs);
    }

    private Properties getKafkaConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private List<TopicPartition> getAllUserTopicPartitions(KafkaConsumerAdapter consumer) {
        final List<TopicPartition> partitions = new ArrayList<>();
        for (final Map.Entry<String, List<PartitionInfo>> entry : consumer.listTopics().entrySet()) {
            final boolean isKafkaInternalTopic = entry.getKey().startsWith("__");
            if (isKafkaInternalTopic) {
                continue;
            }

            for (final PartitionInfo info : entry.getValue()) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

    private void seekToTailRecords(
            KafkaConsumerAdapter consumer,
            List<TopicPartition> partitions,
            Map<TopicPartition, Long> highWatermarks) {
        for (final TopicPartition tp : partitions) {
            final long highWaterMark = highWatermarks.get(tp);
            if (highWaterMark > 0) {
                consumer.seek(tp, highWaterMark - 1);
            }
        }
    }

    private Map<String, Long> collectLatestSeqNums(
            KafkaConsumerAdapter consumer,
            List<TopicPartition> assignedPartitions,
            Map<TopicPartition, Long> highWatermarks,
            long timeoutMs) {

        final long nonEmptyCount = assignedPartitions.stream()
                .filter(tp -> highWatermarks.getOrDefault(tp, 0L) > 0)
                .count();

        final Map<String, Long> chronicleIdToSn = new HashMap<>();
        final Set<TopicPartition> seenPartitions = new HashSet<>();
        final long deadline = System.currentTimeMillis() + timeoutMs;

        while (seenPartitions.size() < nonEmptyCount) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException(
                        "Bootstrap timed out; only saw " + seenPartitions.size()
                                + " of " + nonEmptyCount + " non-empty partitions");
            }

            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (final ConsumerRecord<String, String> record : records) {
                final String chronicleId = record.topic();
                final long seqNum = Long.parseLong(record.key());
                chronicleIdToSn.merge(chronicleId, seqNum, Math::max);
                seenPartitions.add(new TopicPartition(record.topic(), record.partition()));
            }
        }

        return chronicleIdToSn;
    }
}