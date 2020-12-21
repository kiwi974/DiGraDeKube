package com.perso.compute.digradekube.value;

import com.perso.compute.digradekube.SparseVectorProtoString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamObserverSparseVectorStringBuilder {

    private static final Logger LOGGER = Logger.getLogger(StreamObserverSparseVectorStringBuilder.class.getName());
    private static final CountDownLatch FINISH_LATCH = new CountDownLatch(1);

    private Queue<SparseVector> cache;

    public StreamObserverSparseVectorStringBuilder(Queue<SparseVector> cache) {
        this.cache = cache;
    }

    public StreamObserver<SparseVectorProtoString> build() {
        return new StreamObserver<SparseVectorProtoString>() {
            @Override
            public void onNext(SparseVectorProtoString sparseVector) {
                LOGGER.info("[ON NEXT] Called with : " + sparseVector.getVector());
                cache.add(new SparseVector(sparseVector.getVector()));
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.log(Level.WARNING, "RouteChat Failed: {0}", Status.fromThrowable(throwable));
                FINISH_LATCH.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.log(Level.INFO, "Finished NodeExchanger");
                FINISH_LATCH.countDown();
            }
        };
    }
}
