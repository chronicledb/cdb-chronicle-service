package io.github.grantchen2003.cdb.chronicle;

import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxRequest;
import io.github.grantchen2003.cdb.chronicle.grpc.AppendTxResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ChronicleServiceImplTest {
    private static final String dummyBootstrapServers = "localhost:9092";
    private static final int NUM_EXECUTOR_THREADS = 50;
    private ChronicleServiceImpl service;
    private ExecutorService executor;
    private ChronicleLogProducerStub logProducer;

    @BeforeEach
    void setUp() {
        final Map<String, Long> cdbIdToSn = ChronicleSnBootstrapper.loadCdbIdSeqNums(dummyBootstrapServers);
        logProducer = new ChronicleLogProducerStub();
        service = new ChronicleServiceImpl(cdbIdToSn, logProducer);
        executor = Executors.newFixedThreadPool(NUM_EXECUTOR_THREADS);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testAppendTx_ConcurrentRequestsToSameCdbOnlyOneSucceeds() throws InterruptedException {
        final int numConcurrentRequests = NUM_EXECUTOR_THREADS;

        for (int i = 0; i < 100; i++) {
            final String cdbId = "cdb_" + i;
            final long targetSn = 1L;

            final AppendTxRequest request = AppendTxRequest.newBuilder()
                    .setCdbId(cdbId)
                    .setSeqNum(targetSn)
                    .build();

            final CountDownLatch startGate = new CountDownLatch(1);
            final CountDownLatch finishGate = new CountDownLatch(numConcurrentRequests);

            final List<AppendTxResponseStub> stubs = new ArrayList<>();

            for (int j = 0; j < numConcurrentRequests; j++) {
                AppendTxResponseStub stub = new AppendTxResponseStub(finishGate);
                stubs.add(stub);

                executor.submit(() -> {
                    try {
                        startGate.await();
                        service.appendTx(request, stub);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            startGate.countDown();

            if (!finishGate.await(10, TimeUnit.SECONDS)) {
                Assertions.fail("High contention test timed out at iteration " + i);
            }

            final List<AppendTxResponse> allResponses = stubs.stream()
                    .map(AppendTxResponseStub::getResponse)
                    .toList();

            final List<AppendTxResponse> successResponses = allResponses.stream()
                    .filter(AppendTxResponse::getSuccess)
                    .toList();

            final List<AppendTxResponse> failureResponses = allResponses.stream()
                    .filter(r -> !r.getSuccess())
                    .toList();

            Assertions.assertEquals(1, successResponses.size(), "Exactly one request should succeed for " + cdbId);
            final AppendTxResponse success = successResponses.getFirst();
            Assertions.assertTrue(success.getSuccess(), "Field 'success' must be true");
            Assertions.assertEquals(targetSn, success.getCommittedSeqNum(), "CommittedSeqNum should match targetSn");
            Assertions.assertTrue(success.getErrorMessage().isEmpty(), "Error message should be empty on success");

            Assertions.assertEquals(numConcurrentRequests - 1, failureResponses.size());
            for (final AppendTxResponse failure : failureResponses) {
                Assertions.assertFalse(failure.getSuccess(), "Field 'success' must be false");
                Assertions.assertEquals(targetSn, failure.getCommittedSeqNum(), "Failure committedSeqNum should be " + targetSn);
                Assertions.assertEquals("Retryable error", failure.getErrorMessage());
            }
        }
    }

    @Test
    void testAppendTx_ConcurrentRequestsToTwoDifferentCdbBothSucceed() throws InterruptedException {
        final String cdb1 = "cdb1";
        final String cdb2 = "cdb2";

        final AppendTxRequest req1 = AppendTxRequest.newBuilder()
                .setCdbId(cdb1)
                .setSeqNum(1)
                .build();

        final AppendTxRequest req2 = AppendTxRequest.newBuilder()
                .setCdbId(cdb2)
                .setSeqNum(1)
                .build();

        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch finishGate = new CountDownLatch(2);

        final AppendTxResponseStub stub1 = new AppendTxResponseStub(finishGate);
        final AppendTxResponseStub stub2 = new AppendTxResponseStub(finishGate);

        executor.submit(() -> {
            try {
                startGate.await();
                service.appendTx(req1, stub1);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        executor.submit(() -> {
            try {
                startGate.await();
                service.appendTx(req2, stub2);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        startGate.countDown();

        if (!finishGate.await(5, TimeUnit.SECONDS)) {
            Assertions.fail("Different ID test timed out");
        }

        final AppendTxResponse resp1 = stub1.getResponse();
        Assertions.assertTrue(resp1.getSuccess(),"cdb1 should succeed independently of cdb2");
        Assertions.assertEquals(1L, resp1.getCommittedSeqNum(),"CommittedSeqNum for cdb1 should match the requested SN");
        Assertions.assertTrue(resp1.getErrorMessage().isEmpty(),"Error message for cdb1 should be empty on success, but was: " + resp1.getErrorMessage());

        final AppendTxResponse resp2 = stub2.getResponse();
        Assertions.assertTrue(resp2.getSuccess(),"cdb2 should succeed independently of cdb1");
        Assertions.assertEquals(1L, resp2.getCommittedSeqNum(),"CommittedSeqNum for cdb2 should match the requested SN");
        Assertions.assertTrue(resp2.getErrorMessage().isEmpty(),"Error message for cdb2 should be empty on success, but was: " + resp2.getErrorMessage());
    }

    @Test
    void testAppendTx_PersistenceFailureDoesNotIncrementSequence() {
        final String cdbId = "test-cdb";

        // 1. Simulate a Kafka Timeout/Failure
        logProducer.setShouldFail(true);

        final AppendTxRequest failReq = AppendTxRequest.newBuilder()
                .setCdbId(cdbId)
                .setSeqNum(1)
                .setTx("data-1")
                .build();

        final AppendTxResponseStub failStub = new AppendTxResponseStub(new CountDownLatch(1));
        service.appendTx(failReq, failStub);

        final AppendTxResponse failResponse = failStub.getResponse();
        Assertions.assertFalse(failResponse.getSuccess(), "Should fail when producer fails");
        Assertions.assertEquals("Persistence failure", failResponse.getErrorMessage());
        Assertions.assertEquals(0L, failResponse.getCommittedSeqNum(), "Committed SN should still be 0");

        // 2. Fix Kafka and try the SAME sequence number again
        logProducer.setShouldFail(false);

        final AppendTxRequest successReq = AppendTxRequest.newBuilder()
                .setCdbId(cdbId)
                .setSeqNum(1)
                .setTx("data-1-retry")
                .build();

        final AppendTxResponseStub successStub = new AppendTxResponseStub(new CountDownLatch(1));
        service.appendTx(successReq, successStub);

        final AppendTxResponse successResponse = successStub.getResponse();
        Assertions.assertTrue(successResponse.getSuccess(), "Should succeed now that Kafka is up");
        Assertions.assertEquals(1L, successResponse.getCommittedSeqNum(), "SN 1 should now be committed");
    }
}