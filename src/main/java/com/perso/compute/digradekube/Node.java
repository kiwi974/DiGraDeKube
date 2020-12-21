package com.perso.compute.digradekube;

import com.perso.compute.digradekube.service.NodeServer;
import com.perso.compute.digradekube.thread.Listener;
import com.perso.compute.digradekube.thread.Server;
import com.perso.compute.digradekube.utils.DigradeKubeCmd;
import com.perso.compute.digradekube.value.SparseVector;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Node {

    private static final Logger LOGGER = Logger.getLogger(Node.class.getName());

    private static Queue<SparseVector> cache = new LinkedList<SparseVector>();
    private static boolean converged = false;
    private static final Random RAND = new Random();
    private static List<String> neighborsMap = new ArrayList<>();
    private static List<StreamObserver<SparseVectorProtoString>> neighborsStreams = new ArrayList<>();

    /**
     * Main method of the Node :
     *  - one thread to listen other nodes on a port.
     *  - one thread to send updates to other nodes.
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        /* Read arguments */
        CommandLine cmd = DigradeKubeCmd.readCmd(args);
        String self_id= cmd.getOptionValue("self");
        int self_port = Integer.parseInt(self_id.split(":")[1]);
        String neighbors_id = cmd.getOptionValue("neighbors", "");

        /* Build neighbors map */
        if (!neighbors_id.equals("")) {
            neighborsMap.addAll(Arrays.asList(neighbors_id.split(",")));
        }

        /* Start server thread */
        Thread serverThread = new Thread(new Server(self_port, neighborsStreams, cache));
        serverThread.start();

        TimeUnit.SECONDS.sleep(5);

        /* Start thread to listen other worker nodes.*/
        Thread listenerThread = new Thread(new Listener(cache, neighborsMap, neighborsStreams));
        listenerThread.start();

        /* Compute */
        SparseVector currentElement = new SparseVector(3);
        currentElement.put(0, RAND.nextInt());
        LOGGER.info("[INIT] Initial element is : " + currentElement);

        while (!converged) {
            if (!cache.isEmpty()) {
                currentElement = cache.remove();
                LOGGER.info(String.format("[CACHE-OK] Took element %s in the cache.", currentElement));
            } else {
                LOGGER.info("[CACHE-KO] There is no element in the cache.");
            }
            currentElement.populate();
            LOGGER.info("[NODE] Generating element : " + currentElement.toString());
            /* Send to neighbors */
            for (StreamObserver<SparseVectorProtoString> streamObserver : neighborsStreams) {
                LOGGER.info("[COMM] Dealing with : " + streamObserver.toString());
                String stringifyElement = currentElement.toString();
                SparseVectorProtoString sparseVectorProtoString = SparseVectorProtoString.newBuilder().setVector(stringifyElement).build();
                streamObserver.onNext(sparseVectorProtoString);
            }
            TimeUnit.SECONDS.sleep(5);
            converged = currentElement.norm1() > 30;
        }

        if (converged) {
            /* Terminate neighbors streams */
        }

        LOGGER.info("Convergence condition reached with element : " + currentElement + " (norm1 = " + currentElement.norm1() + ")");
        listenerThread.interrupt();

    }

}
