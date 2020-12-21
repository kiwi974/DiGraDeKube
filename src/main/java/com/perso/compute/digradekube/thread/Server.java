package com.perso.compute.digradekube.thread;

import com.perso.compute.digradekube.SparseVectorProtoString;
import com.perso.compute.digradekube.service.NodeServer;
import com.perso.compute.digradekube.value.SparseVector;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

public class Server implements Runnable{

    private static int port;
    private static List<StreamObserver<SparseVectorProtoString>> neighborsStreams;
    private static Queue<SparseVector> cache;

    public Server(int port, List<StreamObserver<SparseVectorProtoString>> neighborsStreams, Queue<SparseVector> cache)  {
        this.port = port;
        this.neighborsStreams = neighborsStreams;
        this.cache = cache;
    }

    @Override
    public void run() {
        NodeServer server = new NodeServer(port, neighborsStreams, cache);
        try {
            server.start();
            server.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
