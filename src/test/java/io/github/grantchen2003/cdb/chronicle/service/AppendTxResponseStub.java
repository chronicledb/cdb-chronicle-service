package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

public class AppendTxResponseStub implements StreamObserver<AppendTxResponse> {
    private AppendTxResponse response;
    private StatusRuntimeException error;
    private final CountDownLatch latch;

    public AppendTxResponseStub(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onNext(AppendTxResponse value) {
        this.response = value;
    }

    @Override
    public void onError(Throwable t) {
        if (t instanceof StatusRuntimeException sre) {
            this.error = sre;
        }
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public AppendTxResponse getResponse() {
        return response;
    }

    public StatusRuntimeException getError() {
        return error;
    }

    public boolean isSuccess() {
        return error == null;
    }
}