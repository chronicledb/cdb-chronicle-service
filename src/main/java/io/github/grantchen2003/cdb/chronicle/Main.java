package io.github.grantchen2003.cdb.chronicle;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        final int port = Integer.parseInt(EnvConfig.get("CHRONICLE_SERVICE_PORT"));
        final String kafkaBootstrapServers = EnvConfig.get("KAFKA_BOOTSTRAP_SERVERS");

        final ChronicleSnBootstrapper bootstrapper = new ChronicleSnBootstrapper(kafkaBootstrapServers);
        final Map<String, Long> chronicleIdToSn = bootstrapper.loadChronicleIdSeqNums();
        final ChronicleLogProducer logProducer = new KafkaChronicleLogProducer(kafkaBootstrapServers);

        final Server server = ServerBuilder.forPort(port)
                .addService(new ChronicleServiceImpl(chronicleIdToSn, logProducer))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            System.out.println("Stopped cdb-chronicle-service...");

            logProducer.close();
            System.out.println("cdb-chronicle-log producer flushed and closed.");
        }));

        server.start();
        System.out.println("cdb-chronicle-service started on port " + port);
        server.awaitTermination();
    }
}