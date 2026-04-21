package io.github.grantchen2003.cdb.chronicle.config;

import io.github.grantchen2003.cdb.chronicle.bootstrap.ChronicleSeqNumBootstrapper;
import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.producer.KafkaChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.service.ChronicleLogWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class ChronicleLogKafkaConfig {

    @Value("${cdb.chronicle.log.kafka-bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ChronicleLogProducer chronicleLogProducer() {
        return new KafkaChronicleLogProducer(bootstrapServers);
    }

    @Bean
    public Map<String, Long> initialSeqNums() {
        return new ChronicleSeqNumBootstrapper(bootstrapServers).loadChronicleIdSeqNums();
    }

    @Bean
    public ChronicleLogWriter chronicleLogWriter(
            ChronicleLogProducer producer,
            Map<String, Long> initialSeqNums) {
        return new ChronicleLogWriter(producer, initialSeqNums);
    }
}
