package io.github.grantchen2003.cdb.chronicle.service;

import io.github.grantchen2003.cdb.chronicle.producer.ChronicleLogProducer;
import io.github.grantchen2003.cdb.chronicle.service.ChronicleLogWriter.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ChronicleLogWriterTest {
    private ChronicleLogWriter writer;
    private ChronicleLogProducerStub producerStub;

    @BeforeEach
    void setUp() {
        producerStub = new ChronicleLogProducerStub();
        writer = new ChronicleLogWriter(producerStub, new HashMap<>());
    }

    // -----------------------------------------------------------------------
    // Success
    // -----------------------------------------------------------------------

    @Test
    void write_firstTxSucceeds() {
        assertInstanceOf(WriteResult.Success.class, write("c1", 1, "tx"));
    }

    @Test
    void write_sequentialTxsSucceed() {
        write("c1", 1, "tx1");
        assertInstanceOf(WriteResult.Success.class, write("c1", 2, "tx2"));
    }

    @Test
    void write_independentChroniclesDoNotInterfere() {
        assertInstanceOf(WriteResult.Success.class, write("c1", 1, "tx"));
        assertInstanceOf(WriteResult.Success.class, write("c2", 1, "tx"));
    }

    // -----------------------------------------------------------------------
    // SequenceMismatch
    // -----------------------------------------------------------------------

    @Test
    void write_wrongSeqNumReturnsSequenceMismatch() {
        final WriteResult result = write("c1", 99, "tx");

        assertInstanceOf(WriteResult.SequenceMismatch.class, result);
        final WriteResult.SequenceMismatch mismatch = (WriteResult.SequenceMismatch) result;
        assertEquals(1L, mismatch.expected());
        assertEquals(99L, mismatch.received());
    }

    @Test
    void write_repeatingCommittedSeqNumReturnsSequenceMismatch() {
        write("c1", 1, "tx1");
        assertInstanceOf(WriteResult.SequenceMismatch.class, write("c1", 1, "tx1-again"));
    }

    @Test
    void write_skippingSeqNumReturnsSequenceMismatch() {
        write("c1", 1, "tx1");
        final WriteResult result = write("c1", 3, "tx3");

        assertInstanceOf(WriteResult.SequenceMismatch.class, result);
        assertEquals(2L, ((WriteResult.SequenceMismatch) result).expected());
    }

    // -----------------------------------------------------------------------
    // SendFailure
    // -----------------------------------------------------------------------

    @Test
    void write_producerFailureReturnsSendFailure() {
        producerStub.setShouldFail(true);
        assertInstanceOf(WriteResult.SendFailure.class, write("c1", 1, "tx"));
    }

    @Test
    void write_seqNumNotAdvancedAfterSendFailure() {
        producerStub.setShouldFail(true);
        write("c1", 1, "tx");

        producerStub.setShouldFail(false);
        assertInstanceOf(WriteResult.Success.class, write("c1", 1, "tx-retry"));
    }

    @Test
    void write_chronicleIsUnlockedAfterSendFailure() {
        producerStub.setShouldFail(true);
        write("c1", 1, "tx");

        producerStub.setShouldFail(false);
        write("c1", 1, "tx-retry");
        assertInstanceOf(WriteResult.Success.class, write("c1", 2, "tx2"));
    }

    // -----------------------------------------------------------------------
    // WriteInProgress
    // -----------------------------------------------------------------------

    @Test
    void write_concurrentWriteToSameChronicleReturnsWriteInProgress() throws InterruptedException {
        final CountDownLatch sendStarted = new CountDownLatch(1);
        final CountDownLatch sendBlocked = new CountDownLatch(1);
        producerStub.setBlockingFutureHooks(sendStarted, sendBlocked);

        final AtomicReference<WriteResult> secondResult = new AtomicReference<>();
        final CountDownLatch secondDone = new CountDownLatch(1);

        final Thread first = new Thread(() -> write("c1", 1, "tx1"));
        final Thread second = new Thread(() -> {
            secondResult.set(write("c1", 1, "tx1-concurrent"));
            secondDone.countDown();
        });

        first.start();
        assertTrue(sendStarted.await(2, TimeUnit.SECONDS), "First write did not start");

        second.start();
        assertTrue(secondDone.await(2, TimeUnit.SECONDS), "Second write did not complete");

        assertInstanceOf(WriteResult.WriteInProgress.class, secondResult.get());

        sendBlocked.countDown();
        first.join(2_000);
    }

    @Test
    void write_afterInFlightCompletesNextWriteSucceeds() throws InterruptedException {
        final CountDownLatch sendStarted = new CountDownLatch(1);
        final CountDownLatch sendBlocked = new CountDownLatch(1);
        producerStub.setBlockingFutureHooks(sendStarted, sendBlocked);

        final CountDownLatch firstDone = new CountDownLatch(1);
        final Thread first = new Thread(() -> {
            write("c1", 1, "tx1");
            firstDone.countDown();
        });

        first.start();
        assertTrue(sendStarted.await(2, TimeUnit.SECONDS));

        sendBlocked.countDown();
        assertTrue(firstDone.await(2, TimeUnit.SECONDS));

        assertInstanceOf(WriteResult.Success.class, write("c1", 2, "tx2"));
    }

    // -----------------------------------------------------------------------
    // Concurrency - striped locking
    // -----------------------------------------------------------------------

    @Test
    void write_concurrentWritesToDifferentChroniclesAllSucceed() {
        final int numChronicles = 100;
        final ExecutorService pool = Executors.newFixedThreadPool(numChronicles);
        final List<CompletableFuture<WriteResult>> futures = new ArrayList<>();

        for (int i = 0; i < numChronicles; i++) {
            final String chronicleId = "chronicle-" + i;
            futures.add(CompletableFuture.supplyAsync(() -> write(chronicleId, 1, "tx"), pool));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (final CompletableFuture<WriteResult> f : futures) {
            assertInstanceOf(WriteResult.Success.class, f.join());
        }

        pool.shutdown();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Bridges the callback-based write() into a blocking call for test convenience. */
    private WriteResult write(String chronicleId, long seqNum, String tx) {
        final CompletableFuture<WriteResult> future = new CompletableFuture<>();
        writer.write(chronicleId, seqNum, tx, future::complete);
        return future.join();
    }

    // -----------------------------------------------------------------------
    // Stub
    // -----------------------------------------------------------------------

    private static class ChronicleLogProducerStub implements ChronicleLogProducer {
        private final java.util.concurrent.atomic.AtomicBoolean shouldFail =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        private CountDownLatch sendStarted;
        private CountDownLatch sendBlocked;

        void setShouldFail(boolean fail) { shouldFail.set(fail); }

        void setBlockingFutureHooks(CountDownLatch sendStarted, CountDownLatch sendBlocked) {
            this.sendStarted = sendStarted;
            this.sendBlocked = sendBlocked;
        }

        @Override
        public CompletableFuture<Void> sendAsync(String chronicleId, long seqNum, String tx) {
            if (shouldFail.get()) {
                return CompletableFuture.failedFuture(new RuntimeException("Simulated failure"));
            }
            if (sendStarted != null) {
                final CountDownLatch started = sendStarted;
                final CountDownLatch blocked = sendBlocked;
                sendStarted = null;
                sendBlocked = null;
                return CompletableFuture.runAsync(() -> {
                    started.countDown();
                    try { blocked.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }
}