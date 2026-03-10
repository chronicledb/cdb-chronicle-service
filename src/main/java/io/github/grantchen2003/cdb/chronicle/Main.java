package io.github.grantchen2003.cdb.chronicle;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final int port = 9090;

        final ChronicleLogProducer logProducer = new ChronicleLogProducer("localhost:9092");

        final Server server = ServerBuilder.forPort(port)
                .addService(new ChronicleServiceImpl(logProducer))
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
