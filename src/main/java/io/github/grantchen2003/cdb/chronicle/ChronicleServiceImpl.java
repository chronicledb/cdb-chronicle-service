package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {
    private static final int STRIPE_COUNT = 1024;
    private final Map<String, Long> cdbIdToSn;
    private final ChronicleLogProducer chronicleLogProducer;
    private final ReentrantLock[] lockStripes;

    public ChronicleServiceImpl(Map<String, Long> cdbIdToSn, ChronicleLogProducer chronicleLogProducer) {
        this.cdbIdToSn = cdbIdToSn;
        this.chronicleLogProducer = chronicleLogProducer;
        lockStripes = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            lockStripes[i] = new ReentrantLock();
        }
    }

    @Override
    public void appendTx(AppendTxRequest request, StreamObserver<AppendTxResponse> responseObserver) {
        final String cdbId = request.getCdbId();
        final long incomingSn = request.getSeqNum();
        final String tx = request.getTx();

        final AppendTxResponse.Builder responseBuilder = AppendTxResponse.newBuilder();

        final ReentrantLock lock = lockStripes[Math.floorMod(cdbId.hashCode(), STRIPE_COUNT)];
        lock.lock();

        try {
            final long currentSn = cdbIdToSn.getOrDefault(cdbId, 0L);

            if (incomingSn == currentSn + 1) {
                try {
                    chronicleLogProducer.sendSync(cdbId, incomingSn, tx);
                    cdbIdToSn.put(cdbId, incomingSn);
                    responseBuilder.setSuccess(true)
                            .setCommittedSeqNum(incomingSn);
                } catch (ExecutionException | InterruptedException | TimeoutException e) {
                    responseBuilder.setSuccess(false)
                            .setCommittedSeqNum(currentSn)
                            .setErrorMessage("Persistence failure");
                }
            } else {
                responseBuilder.setSuccess(false)
                        .setCommittedSeqNum(currentSn)
                        .setErrorMessage("Retryable error");
            }
        } finally {
            lock.unlock();
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}