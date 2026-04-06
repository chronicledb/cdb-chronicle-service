package io.github.grantchen2003.cdb.chronicle.bootstrap;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KafkaConsumerAdapterStub implements KafkaConsumerAdapter {
    private final Map<String, List<PartitionInfo>> topicMetadata;
    private final Map<TopicPartition, Long> endOffsets;
    private final List<ConsumerRecords<String, String>> pollSequence;
    private int pollCount = 0;

    public KafkaConsumerAdapterStub(
            Map<String, List<PartitionInfo>> topicMetadata,
            Map<TopicPartition, Long> endOffsets,
            List<ConsumerRecords<String, String>> pollSequence) {
        this.topicMetadata = topicMetadata;
        this.endOffsets = endOffsets;
        this.pollSequence = pollSequence;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() { return topicMetadata; }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) { return endOffsets; }

    @Override
    public ConsumerRecords<String, String> poll(Duration timeout) {
        if (pollCount < pollSequence.size()) return pollSequence.get(pollCount++);
        return ConsumerRecords.empty();
    }

    @Override public void assign(Collection<TopicPartition> partitions) {}
    @Override public void seek(TopicPartition partition, long offset) {}
}