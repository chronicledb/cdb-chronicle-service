package io.github.grantchen2003.cdb.chronicle.producer;

import java.util.concurrent.CompletableFuture;

public interface ChronicleLogProducer {
    CompletableFuture<Void> sendAsync(String chronicleId, long seqNum, String tx);
    void close();
}