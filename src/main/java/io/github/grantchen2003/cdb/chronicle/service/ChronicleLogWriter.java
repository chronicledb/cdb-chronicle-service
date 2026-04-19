package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

        long lastCommittedSeqNum();

        record Success(long lastCommittedSeqNum) implements WriteResult {}
        record SendFailure(long lastCommittedSeqNum) implements WriteResult {}
        record WriteInProgress(long lastCommittedSeqNum) implements WriteResult {}
        record SequenceMismatch(long lastCommittedSeqNum, long received) implements WriteResult {}
    }

    public void write(String chronicleId, long incomingSn, String tx, Consumer<WriteResult> callback) {
        final ReentrantLock lock = lockStripe(chronicleId);
        lock.lock();
        try {
            final long currentSn = seqNums.getOrDefault(chronicleId, 0L);

            if (inFlightWrites.contains(chronicleId)) {
                callback.accept(new WriteResult.WriteInProgress(currentSn));
                return;
            }

            if (incomingSn != currentSn + 1) {
                callback.accept(new WriteResult.SequenceMismatch(currentSn, incomingSn));
                return;
            }

            inFlightWrites.add(chronicleId);
            logProducer.sendAsync(chronicleId, incomingSn, tx).whenComplete((ignored, ex) -> {
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
                callback.accept(ex == null
                        ? new WriteResult.Success(incomingSn)
                        : new WriteResult.SendFailure(currentSn));
            });
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock lockStripe(String chronicleId) {
        return lockStripes[Math.floorMod(chronicleId.hashCode(), STRIPE_COUNT)];
    }
}