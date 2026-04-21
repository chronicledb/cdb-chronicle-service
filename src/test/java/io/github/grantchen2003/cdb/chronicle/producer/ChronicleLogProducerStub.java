package io.github.grantchen2003.cdb.chronicle.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleLogProducerStub implements ChronicleLogProducer {
    private final AtomicBoolean shouldFail = new AtomicBoolean(false);

    public void setShouldFail(boolean fail) {
        this.shouldFail.set(fail);
    }

    @Override
    public CompletableFuture<Void> sendAsync(String chronicleId, long seqNum, String tx) {
        if (shouldFail.get()) {
            return CompletableFuture.failedFuture(new RuntimeException("Simulated Kafka failure"));
        }
        return CompletableFuture.completedFuture(null);
    }
}