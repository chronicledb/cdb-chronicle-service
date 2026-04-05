package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {
    private static final int STRIPE_COUNT = 1024;

    private final ConcurrentHashMap<String, CompletableFuture<Void>> chronicleIdToInFlightWrite = new ConcurrentHashMap<>();
    private final Map<String, Long> chronicleIdToSn;
    private final ChronicleLogProducer chronicleLogProducer;
    private final ReentrantLock[] lockStripes;

    public ChronicleServiceImpl(Map<String, Long> chronicleIdToSn, ChronicleLogProducer chronicleLogProducer) {
        this.chronicleIdToSn = chronicleIdToSn;
        this.chronicleLogProducer = chronicleLogProducer;
        lockStripes = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            lockStripes[i] = new ReentrantLock();
        }
    }

    @Override
    public void appendTx(AppendTxRequest request, StreamObserver<AppendTxResponse> responseObserver) {
        final String chronicleId = request.getChronicleId();
        final long incomingSn = request.getSeqNum();
        final String tx = request.getTx();

        final ReentrantLock lock = lockStripes[Math.floorMod(chronicleId.hashCode(), STRIPE_COUNT)];

        final CompletableFuture<Void> sendFuture;
        lock.lock();
        try {
            if (chronicleIdToInFlightWrite.containsKey(chronicleId)) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Previous write still in-flight, retry")
                        .asRuntimeException());
                return;
            }

            final long currentSn = chronicleIdToSn.getOrDefault(chronicleId, 0L);
            if (incomingSn != currentSn + 1) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Sequence number mismatch; expected "
                                + (currentSn + 1) + ", got " + incomingSn)
                        .asRuntimeException());
                return;
            }

            // register in-flight *before* releasing the lock
            sendFuture = chronicleLogProducer.sendAsync(chronicleId, incomingSn, tx);
            chronicleIdToInFlightWrite.put(chronicleId, sendFuture);
        } finally {
            lock.unlock();
        }

        sendFuture.whenComplete((ignored, ex) -> {
            lock.lock();
            try {
                chronicleIdToInFlightWrite.remove(chronicleId);
                if (ex == null) {
                    // only advance seqNum on confirmed write
                    chronicleIdToSn.put(chronicleId, incomingSn);
                }
            } finally {
                lock.unlock();
            }

            if (ex != null) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Persistence failure")
                        .asRuntimeException());
            } else {
                responseObserver.onNext(AppendTxResponse.newBuilder()
                        .setCommittedSeqNum(incomingSn)
                        .build());
                responseObserver.onCompleted();
            }
        });
    }
}