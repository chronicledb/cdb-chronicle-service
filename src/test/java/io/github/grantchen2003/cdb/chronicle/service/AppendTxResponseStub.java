package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

class AppendTxResponseStub implements StreamObserver<AppendTxResponse> {
    private final CountDownLatch latch;
    private AppendTxResponse response;

    AppendTxResponseStub(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onNext(AppendTxResponse response) {
        this.response = response;
    }

    @Override
    public void onError(Throwable t) {
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public AppendTxResponse getResponse() {
        return response;
    }

    public boolean isSuccess() {
        return response != null && response.getSuccess();
    }
}