package io.github.grantchen2003.cdb.chronicle;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleLogProducerStub extends ChronicleLogProducer {
    private final AtomicBoolean shouldFail = new AtomicBoolean(false);

    public ChronicleLogProducerStub(String bootstrapServers) {
        super(bootstrapServers);
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