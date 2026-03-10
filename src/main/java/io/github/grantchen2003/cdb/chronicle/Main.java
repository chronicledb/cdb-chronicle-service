package io.github.grantchen2003.cdb.chronicle;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final int port = 9090;

        final Server server = ServerBuilder.forPort(port)
                .addService(new ChronicleServiceImpl())
                .build();

        System.out.println("cdb-chronicle-service started on port " + port);

        server.start();
        server.awaitTermination();
    }
}
