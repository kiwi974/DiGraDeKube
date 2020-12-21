package com.perso.compute.digradekube.thread;

import com.perso.compute.digradekube.NodeExchangerGrpc;
import com.perso.compute.digradekube.SparseVectorProtoString;
import com.perso.compute.digradekube.value.SparseVector;
import com.perso.compute.digradekube.value.StreamObserverSparseVectorStringBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.logging.Logger;

public class Listener implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Listener.class.getName());
    private static final Random RAND = new Random();

    private Queue<SparseVector> vectorsCache;
    private List<String> neighborsMap;
    private List<StreamObserver<SparseVectorProtoString>> neighborsStreams;
    private List<String> seenNeighbors;

    public Listener(Queue<SparseVector> vectorsCache, List<String> neighborsMap, List<StreamObserver<SparseVectorProtoString>> neighborsStreams) {
        this.vectorsCache = vectorsCache;
        this.neighborsMap = neighborsMap;
        this.neighborsStreams = neighborsStreams;
        this.seenNeighbors = new ArrayList<>();
    }

    @Override
    public void run() {
        while(true) {
            /* Check if their is a new neighbor in the map */
            for (String neighbor : this.neighborsMap) {
                if (!this.seenNeighbors.contains(neighbor)) {
                    /* Open channel with this new neighbor */
                    ManagedChannel channel = ManagedChannelBuilder.forTarget(neighbor).usePlaintext().build();
                    NodeExchangerGrpc.NodeExchangerStub asyncStub = NodeExchangerGrpc.newStub(channel);
                    StreamObserver<SparseVectorProtoString> requestObserver = new StreamObserverSparseVectorStringBuilder(this.vectorsCache).build();
                    LOGGER.info("[LISTENER] Generated observer : " + requestObserver);
                    StreamObserver<SparseVectorProtoString> responseObserver = asyncStub.sendVector(requestObserver);
                    LOGGER.info("[LISTENER] Got observer : " + responseObserver);
                    /* Put the new streamObserver in the dedicated cache of the node */
                    this.neighborsStreams.add(responseObserver);
                    /* Mark this new neighbor as seen */
                    this.seenNeighbors.add(neighbor);
                }
            }
        }
    }
}