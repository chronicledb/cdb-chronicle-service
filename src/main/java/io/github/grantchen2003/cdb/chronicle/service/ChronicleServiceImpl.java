package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.service.ChronicleLogWriter.WriteResult;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class ChronicleServiceImpl extends ChronicleServiceGrpc.ChronicleServiceImplBase {
    private final ChronicleLogWriter chronicleLogWriter;

    public ChronicleServiceImpl(Map<String, Long> initialSeqNums, ChronicleLogProducer logProducer) {
        this.chronicleLogWriter = new ChronicleLogWriter(logProducer, initialSeqNums);
    }

    @Override
    public void appendTx(AppendTxRequest request, StreamObserver<AppendTxResponse> responseObserver) {
        final String chronicleId = request.getChronicleId();
        final long incomingSn = request.getSeqNum();
        final String tx = request.getTx();

        chronicleLogWriter.write(chronicleId, incomingSn, tx, result -> {
            if (result instanceof WriteResult.WriteInProgress) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Previous write still in-flight, retry")
                        .asRuntimeException());

            } else if (result instanceof WriteResult.SequenceMismatch(long expected, long received)) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Sequence number mismatch; expected " + expected + ", got " + received)
                        .asRuntimeException());

            } else if (result instanceof WriteResult.SendFailure) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Persistence failure")
                        .asRuntimeException());

            } else if (result instanceof WriteResult.Success) {
                responseObserver.onNext(AppendTxResponse.newBuilder()
                        .setCommittedSeqNum(incomingSn)
                        .build());
                responseObserver.onCompleted();
            }
        });
    }
}