package io.github.grantchen2003.cdb.chronicle;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        final int port = 9090;
        final String kafkaBootstrapServers = "localhost:9092";

        final Map<String, Long> cdbIdToSn = ChronicleSnBootstrapper.loadCdbIdSeqNums(kafkaBootstrapServers);
        final ChronicleLogProducer logProducer = new ChronicleLogProducer(kafkaBootstrapServers);

        final Server server = ServerBuilder.forPort(port)
                .addService(new ChronicleServiceImpl(cdbIdToSn, logProducer))
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
