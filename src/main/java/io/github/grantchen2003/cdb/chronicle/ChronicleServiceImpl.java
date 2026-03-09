package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {
    private final ConcurrentHashMap<String, Long> cdbIdToSn = new ConcurrentHashMap<>();
    private final ReentrantLock[] lockStripes;
    private final int STRIPE_COUNT = 1024;

    public ChronicleServiceImpl() {
        lockStripes = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            lockStripes[i] = new ReentrantLock();
        }
    }

    @Override
    public void appendTx(AppendTxRequest request, StreamObserver<AppendTxResponse> responseObserver) {
        final String cdbId = request.getCdbId();
        final long incomingSn = request.getSeqNum();

        final AppendTxResponse.Builder responseBuilder = AppendTxResponse.newBuilder();

        final ReentrantLock lock = lockStripes[Math.abs(cdbId.hashCode() % STRIPE_COUNT)];
        lock.lock();

        try {
            long currentSn = cdbIdToSn.getOrDefault(cdbId, 0L);

            if (incomingSn == currentSn + 1) {
                cdbIdToSn.put(cdbId, incomingSn);

                responseBuilder.setSuccess(true)
                        .setCommittedSeqNum(incomingSn);

            } else {
                responseBuilder.setSuccess(false)
                        .setCommittedSeqNum(-1)
                        .setErrorMessage("Retryable error: Invalid sequence number. Current sequence number is " + currentSn);
            }
        } finally {
            lock.unlock();
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}