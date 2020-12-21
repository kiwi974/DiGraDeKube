package com.perso.compute.digradekube.service;

import com.perso.compute.digradekube.NodeExchangerGrpc;
import com.perso.compute.digradekube.SparseVectorProtoString;
import com.perso.compute.digradekube.value.SparseVector;
import com.perso.compute.digradekube.value.StreamObserverSparseVectorStringBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class NodeServer {

    private static final Logger LOGGER = Logger.getLogger(NodeServer.class.getName());

    private final int port;
    private final Server server;

    /**
     * Create a RouteGuide server using serverBuilder as a base and features as data.
     */
    /*public NodeServer(int port) {
        this.port = port;
        server = ServerBuilder.forPort(port).build();
    }*/

    /** Create a RouteGuide server using serverBuilder as a base and features as data. */
    public NodeServer(int port, List<StreamObserver<SparseVectorProtoString>> neighborsStreams, Queue<SparseVector> cache) {
        this.port = port;
        server = ServerBuilder.forPort(port).addService(new NodeExchangerService(neighborsStreams, cache))
                .build();
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        server.start();
        LOGGER.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    NodeServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    /**
     * Stop serving requests and shutdown resources.
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class NodeExchangerService extends NodeExchangerGrpc.NodeExchangerImplBase {

        private List<StreamObserver<SparseVectorProtoString>> neighborsStreams;
        private Queue<SparseVector> cache;

        public NodeExchangerService(List<StreamObserver<SparseVectorProtoString>> neighborsStreams, Queue<SparseVector> cache) {
            this.neighborsStreams = neighborsStreams;
            this.cache = cache;
        }

        @Override
        public StreamObserver<SparseVectorProtoString> sendVector(StreamObserver<SparseVectorProtoString> responseObserver) {
            LOGGER.info("[NODE EXCHANGER] Got stream : " + responseObserver);
            this.neighborsStreams.add(responseObserver);
            return new StreamObserverSparseVectorStringBuilder(this.cache).build();
        }
    }
}
