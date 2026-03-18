package io.github.grantchen2003.cdb.chronicle;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleLogProducerStub implements ChronicleLogProducer {
    private final AtomicBoolean shouldFail = new AtomicBoolean(false);

    public void setShouldFail(boolean fail) {
        this.shouldFail.set(fail);
    }

    @Override
    public void sendSync(String chronicleId, long seqNum, String tx) throws TimeoutException {
        if (shouldFail.get()) {
            throw new TimeoutException("Simulated Kafka Timeout");
        }
    }

    @Override
    public void close() {
    }
}