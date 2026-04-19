package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.service.ChronicleLogWriter.WriteResult;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.github.grantchen2003.cdb.chronicle.grpc.ChronicleServiceGrpc;
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
            final AppendTxResponse response = switch (result) {
                case WriteResult.Success s -> AppendTxResponse.newBuilder()
                        .setSuccess(true)
                        .setCommittedSeqNum(s.lastCommittedSeqNum())
                        .build();

                case WriteResult.WriteInProgress w -> AppendTxResponse.newBuilder()
                        .setSuccess(false)
                        .setCommittedSeqNum(w.lastCommittedSeqNum())
                        .setErrorMessage("Previous write still in-flight, retry")
                        .build();

                case WriteResult.SequenceMismatch m -> AppendTxResponse.newBuilder()
                        .setSuccess(false)
                        .setCommittedSeqNum(m.lastCommittedSeqNum())
                        .setErrorMessage("Sequence number mismatch; expected " + (m.lastCommittedSeqNum() + 1) + ", got " + m.received())
                        .build();

                case WriteResult.SendFailure f -> AppendTxResponse.newBuilder()
                        .setSuccess(false)
                        .setCommittedSeqNum(f.lastCommittedSeqNum())
                        .setErrorMessage("Persistence failure")
                        .build();
            };

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }
}