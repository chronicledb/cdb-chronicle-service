package io.github.grantchen2003.cdb.chronicle.bootstrap;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class KafkaConsumerAdapterImpl implements KafkaConsumerAdapter {
    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerAdapterImpl(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return consumer.listTopics();
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        consumer.assign(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return consumer.endOffsets(partitions);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        consumer.seek(partition, offset);
    }

    @Override
    public ConsumerRecords<String, String> poll(Duration timeout) {
        return consumer.poll(timeout);
    }
}