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
    private Map<Integer, String> neighborsMap;
    private List<StreamObserver<SparseVectorProtoString>> neighborsStreams;
    private List<Map.Entry<Integer, String>> seenNeighbors;

    public Listener(Queue<SparseVector> vectorsCache, Map<Integer, String> neighborsMap, List<StreamObserver<SparseVectorProtoString>> neighborsStreams) {
        this.vectorsCache = vectorsCache;
        this.neighborsMap = neighborsMap;
        this.neighborsStreams = neighborsStreams;
        this.seenNeighbors = new ArrayList<>();
    }

    @Override
    public void run() {
        for (Map.Entry<Integer, String> neighbor : this.neighborsMap.entrySet()) {
            if (!this.seenNeighbors.contains(neighbor)) {
                /* Open channel with this new neighbor */
                String target = neighbor.getValue() + ":" + neighbor.getKey();
                ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
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
