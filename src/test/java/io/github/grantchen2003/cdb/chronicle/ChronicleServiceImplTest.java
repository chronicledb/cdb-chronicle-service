package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ChronicleServiceImplTest {
    private ChronicleServiceImpl service;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        service = new ChronicleServiceImpl();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testConcurrentAppendTx_OneSucceedsOneFails() throws InterruptedException {
        // run 100 times to try to catch flaky concurrency bugs
        for (int i = 0; i < 100; i++) {
            final AppendTxRequest request = AppendTxRequest.newBuilder()
                    .setCdbId("cdb" + i)
                    .setSeqNum(1)
                    .build();

            final CountDownLatch startGate = new CountDownLatch(1);
            final CountDownLatch finishGate = new CountDownLatch(2);

            final AppendTxResponseStub stub1 = new AppendTxResponseStub(finishGate);
            final AppendTxResponseStub stub2 = new AppendTxResponseStub(finishGate);

            executor.submit(() -> {
                try {
                    startGate.await();
                    service.appendTx(request, stub1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            executor.submit(() -> {
                try {
                    startGate.await();
                    service.appendTx(request, stub2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            startGate.countDown();

            if (!finishGate.await(5, TimeUnit.SECONDS)) {
                Assertions.fail("Test timed out - requests did not complete");
            }

            final boolean stub1Success = stub1.getResponse().getSuccess();
            final boolean stub2Success = stub2.getResponse().getSuccess();

            // XOR: One must be true, one must be false
            Assertions.assertTrue(stub1Success ^ stub2Success,
                    "Exactly one thread should have succeeded. S1: " + stub1Success + " S2: " + stub2Success);
        }
    }

    /**
     * Simple Stub for capturing gRPC responses
     */
    static class AppendTxResponseStub implements StreamObserver<AppendTxResponse> {
        private AppendTxResponse response;
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
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }

        public AppendTxResponse getResponse() {
            return response;
        }
    }
}