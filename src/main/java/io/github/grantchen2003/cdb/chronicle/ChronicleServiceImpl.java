package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {
    private static final int STRIPE_COUNT = 1024;
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
        lock.lock();

        try {
            final long currentSn = chronicleIdToSn.getOrDefault(chronicleId, 0L);

            if (incomingSn == currentSn + 1) {
                try {
                    chronicleLogProducer.sendSync(chronicleId, incomingSn, tx);
                    chronicleIdToSn.put(chronicleId, incomingSn);
                    responseObserver.onNext(AppendTxResponse.newBuilder()
                            .setCommittedSeqNum(incomingSn)
                            .build());
                    responseObserver.onCompleted();
                } catch (ExecutionException | InterruptedException | TimeoutException e) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("Persistence failure")
                            .asRuntimeException());
                }
            } else {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Sequence number mismatch; expected " + (currentSn + 1) + ", got " + incomingSn)
                        .asRuntimeException());
            }
        } finally {
            lock.unlock();
        }
    }
}