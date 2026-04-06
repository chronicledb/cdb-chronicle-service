package io.github.grantchen2003.cdb.chronicle.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaChronicleLogProducer implements ChronicleLogProducer {
    private final KafkaProducer<String, String> producer;

    public KafkaChronicleLogProducer(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public CompletableFuture<Void> sendAsync(String chronicleId, long seqNum, String tx) {
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(chronicleId, String.valueOf(seqNum), tx);

        final CompletableFuture<Void> result = new CompletableFuture<>();

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                result.completeExceptionally(exception);
            } else {
                result.complete(null);
            }
        });

        return result;
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(30));
    }
}