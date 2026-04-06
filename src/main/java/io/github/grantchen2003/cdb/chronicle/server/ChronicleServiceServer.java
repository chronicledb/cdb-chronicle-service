package io.github.grantchen2003.cdb.chronicle.server;

import io.github.grantchen2003.cdb.chronicle.bootstrap.ChronicleSeqNumBootstrapper;
import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.producer.KafkaChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.service.ChronicleServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Map;

public class ChronicleServiceServer {
    private final Server grpcServer;
    private final ChronicleLogProducer logProducer;
    private final int port;

    public ChronicleServiceServer(int port, String kafkaBootstrapServers) {
        this.port = port;

        final Map<String, Long> initialSeqNums = new ChronicleSeqNumBootstrapper(kafkaBootstrapServers).loadChronicleIdSeqNums();

        this.logProducer = new KafkaChronicleLogProducer(kafkaBootstrapServers);

        this.grpcServer = ServerBuilder.forPort(port)
                .addService(new ChronicleServiceImpl(initialSeqNums, logProducer))
                .build();
    }

    public void start() throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        grpcServer.start();
        System.out.println("cdb-chronicle-service started on port " + port);
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    private void shutdown() {
        grpcServer.shutdown();
        System.out.println("Stopped cdb-chronicle-service.");

        logProducer.close();
        System.out.println("Chronicle log producer flushed and closed.");
    }
}