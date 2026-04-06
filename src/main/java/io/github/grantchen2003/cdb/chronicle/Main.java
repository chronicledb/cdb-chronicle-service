package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.config.EnvConfig;
import io.github.grantchen2003.cdb.chronicle.server.ChronicleServiceServer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final int port = Integer.parseInt(EnvConfig.get("CHRONICLE_SERVICE_PORT"));
        final String kafkaBootstrapServers = EnvConfig.get("KAFKA_BOOTSTRAP_SERVERS");

        final ChronicleServiceServer server = new ChronicleServiceServer(port, kafkaBootstrapServers);
        server.start();
        server.awaitTermination();
    }
}