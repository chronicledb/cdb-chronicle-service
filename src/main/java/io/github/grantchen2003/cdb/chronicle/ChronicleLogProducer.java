package io.github.grantchen2003.cdb.chronicle;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface ChronicleLogProducer {
    void sendSync(String chronicleId, long seqNum, String tx) throws ExecutionException, InterruptedException, TimeoutException;
    void close();
}