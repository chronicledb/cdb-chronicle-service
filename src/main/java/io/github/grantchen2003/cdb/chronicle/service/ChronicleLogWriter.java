package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class ChronicleLogWriter {
    private static final int STRIPE_COUNT = 1 << 14;

    private final ChronicleLogProducer logProducer;
    private final Set<String> inFlightWrites = new HashSet<>();
    private final Map<String, Long> seqNums;
    private final ReentrantLock[] lockStripes;

    public ChronicleLogWriter(ChronicleLogProducer logProducer, Map<String, Long> seqNums) {
        this.logProducer = logProducer;
        this.seqNums = seqNums;
        this.lockStripes = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            lockStripes[i] = new ReentrantLock();
        }
    }

    public sealed interface WriteResult
            permits WriteResult.Success, WriteResult.SendFailure, WriteResult.WriteInProgress, WriteResult.SequenceMismatch {

        record Success() implements WriteResult {}

        record SendFailure() implements WriteResult {}

        record WriteInProgress() implements WriteResult {}

        record SequenceMismatch(long expected, long received) implements WriteResult {}
    }

    public void write(String chronicleId, long incomingSn, String tx, Consumer<WriteResult> callback) {
        final ReentrantLock lock = lockStripe(chronicleId);
        lock.lock();
        try {
            if (inFlightWrites.contains(chronicleId)) {
                callback.accept(new WriteResult.WriteInProgress());
                return;
            }

            final long currentSn = seqNums.getOrDefault(chronicleId, 0L);
            final long expectedSn = currentSn + 1;
            if (incomingSn != expectedSn) {
                callback.accept(new WriteResult.SequenceMismatch(expectedSn, incomingSn));
                return;
            }

            inFlightWrites.add(chronicleId);
            final CompletableFuture<Void> sendFuture = logProducer.sendAsync(chronicleId, incomingSn, tx);

            sendFuture.whenComplete((ignored, ex) -> {
                lock.lock();
                try {
                    inFlightWrites.remove(chronicleId);
                    if (ex == null) {
                        seqNums.put(chronicleId, incomingSn);
                    } else {
                        ex.printStackTrace();
                    }
                } finally {
                    lock.unlock();
                }
                callback.accept(ex == null ? new WriteResult.Success() : new WriteResult.SendFailure());
            });
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock lockStripe(String chronicleId) {
        return lockStripes[Math.floorMod(chronicleId.hashCode(), STRIPE_COUNT)];
    }
}