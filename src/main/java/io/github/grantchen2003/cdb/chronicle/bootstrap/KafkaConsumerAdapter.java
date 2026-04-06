package io.github.grantchen2003.cdb.chronicle.bootstrap;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface KafkaConsumerAdapter {
    Map<String, List<PartitionInfo>> listTopics();
    void assign(Collection<TopicPartition> partitions);
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);
    void seek(TopicPartition partition, long offset);
    ConsumerRecords<String, String> poll(Duration timeout);
}