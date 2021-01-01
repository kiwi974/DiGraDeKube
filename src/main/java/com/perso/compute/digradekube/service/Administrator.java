package com.perso.compute.digradekube.service;

import com.perso.compute.digradekube.AdministratorGrpc;
import com.perso.compute.digradekube.NodeIdentity;
import com.perso.compute.digradekube.NodeListing;
import com.perso.compute.digradekube.value.SparseVector;
import com.perso.compute.digradekube.value.StreamObserverSparseVectorStringBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Administrator {

    private static final Logger LOGGER = Logger.getLogger(NodeServer.class.getName());

    private int port;
    private Server server;

    /**
     * Create a RouteGuide server using serverBuilder as a base and features as data.
     */
    /*public NodeServer(int port) {
        this.port = port;
        server = ServerBuilder.forPort(port).build();
    }*/

    /** Create a RouteGuide server using serverBuilder as a base and features as data. */
    public Administrator(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port).addService(new AdministratorService())
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
                    Administrator.this.stop();
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

    public static void main(String[] args) {
        Administrator administrator = new Administrator(8980);
        try {
            administrator.start();
            administrator.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class AdministratorService extends AdministratorGrpc.AdministratorImplBase {

        private Map<Integer, String> routingTable = new HashMap<Integer, String>();

        @Override
        public void register(NodeIdentity request, StreamObserver<NodeListing> responseObserver) {
            LOGGER.info("[ADMINISTRATOR] Before enrolment : " + this.routingTable);
            responseObserver.onNext(NodeListing.newBuilder().setList(this.routingTable.toString()).build());
            this.routingTable.put(request.getPort(), request.getName());
            LOGGER.info("[ADMINISTRATOR] After enrolment : " + this.routingTable);
            responseObserver.onCompleted();
        }
    }

}
