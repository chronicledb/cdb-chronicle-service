package io.github.grantchen2003.cdb.chronicle;

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

public class ChronicleSnBootstrapper {
    private static final long DEFAULT_BOOTSTRAP_TIMEOUT_MS = 30_000;

    private final String bootstrapServers;

    public ChronicleSnBootstrapper(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, Long> loadCdbIdSeqNums() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            return loadCdbIdSeqNums(new KafkaConsumerAdapterImpl(consumer), DEFAULT_BOOTSTRAP_TIMEOUT_MS);
        }
    }

    Map<String, Long> loadCdbIdSeqNums(KafkaConsumerAdapter consumer, long timeoutMs) {
        final Map<String, Long> cdbIdToSn = new HashMap<>();

        final Map<String, List<PartitionInfo>> topicToPartitions = consumer.listTopics();

        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (final String topic : topicToPartitions.keySet()) {
            final boolean isKafkaInternalTopic = topic.startsWith("__");
            if (isKafkaInternalTopic) {
                continue;
            }

            for (final PartitionInfo p : topicToPartitions.get(topic)) {
                topicPartitions.add(new TopicPartition(topic, p.partition()));
            }
        }

        consumer.assign(topicPartitions);

        final Map<TopicPartition, Long> partitionToHighWatermark = consumer.endOffsets(topicPartitions);
        for (final TopicPartition tp : topicPartitions) {
            final long highWatermark = partitionToHighWatermark.get(tp);
            final boolean partitionIsNonEmpty = highWatermark > 0;
            if (partitionIsNonEmpty) {
                final long lastRecordOffset = highWatermark - 1;
                consumer.seek(tp, lastRecordOffset);
            }
        }

        final Set<TopicPartition> seenPartitions = new HashSet<>();
        final long nonEmptyTopicPartitionCount = topicPartitions.stream()
                .filter(tp -> partitionToHighWatermark.get(tp) > 0)
                .count();

        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (seenPartitions.size() < nonEmptyTopicPartitionCount) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("Bootstrap timed out; only saw " + seenPartitions.size()
                        + " of " + nonEmptyTopicPartitionCount + " partitions");
            }

            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (final ConsumerRecord<String, String> record : records) {
                final String cdbId = record.topic();
                final long seqNum = Long.parseLong(record.key());

                cdbIdToSn.merge(cdbId, seqNum, Math::max);

                seenPartitions.add(new TopicPartition(record.topic(), record.partition()));
            }
        }

        return cdbIdToSn;
    }
}