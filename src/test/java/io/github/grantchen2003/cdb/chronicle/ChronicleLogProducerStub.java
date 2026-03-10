package io.github.grantchen2003.cdb.chronicle;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleLogProducerStub extends ChronicleLogProducer {
    private final AtomicBoolean shouldFail = new AtomicBoolean(false);
    private static final String dummyAddress = "localhost:9092";

    public ChronicleLogProducerStub() {
        super(dummyAddress);
    }

    public void setShouldFail(boolean fail) {
        this.shouldFail.set(fail);
    }

    @Override
    public void sendSync(String cdbId, long seqNum, String tx) throws TimeoutException {
        if (shouldFail.get()) {
            throw new TimeoutException("Simulated Kafka Timeout");
        }
    }

    @Override
    public void close() {
    }
}