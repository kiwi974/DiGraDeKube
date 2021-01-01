package com.perso.compute.digradekube;

import com.perso.compute.digradekube.service.NodeServer;
import com.perso.compute.digradekube.thread.Listener;
import com.perso.compute.digradekube.thread.Server;
import com.perso.compute.digradekube.utils.DigradeKubeCmd;
import com.perso.compute.digradekube.value.SparseVector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Node {

    private static final Logger LOGGER = Logger.getLogger(Node.class.getName());

    private static final String ADMIN_ID = "localhost:8980";

    private static Queue<SparseVector> cache = new LinkedList<SparseVector>();
    private static boolean converged = false;
    private static final Random RAND = new Random();
    private static Map<Integer, String> neighborsMap;
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
        String self_name = self_id.split(":")[0];
        int self_port = Integer.parseInt(self_id.split(":")[1]);

        /* Declare to administrator */
        ManagedChannel channel = ManagedChannelBuilder.forTarget(ADMIN_ID).usePlaintext().build();
        AdministratorGrpc.AdministratorBlockingStub blockingStub = AdministratorGrpc.newBlockingStub(channel);
        NodeListing nodeListing = blockingStub.register(NodeIdentity.newBuilder().setName(self_name).setPort(self_port).build());

        LOGGER.info("[NODE] NodeListing : " + nodeListing.getList());

        /* Build neighbors map */
        if (!nodeListing.getList().equals("{}")) {
            neighborsMap = (HashMap<Integer, String>)
                    Arrays.asList(nodeListing.getList().substring(1, nodeListing.getList().length()-1).split(","))
                            .stream().map(s -> s.split("=")).collect(Collectors.toMap(e -> Integer.parseInt(e[0].trim()), e -> e[1].trim()));
        } else {
            neighborsMap = Collections.emptyMap();
        }

        LOGGER.info("[NODE] Build neighbors map : " + neighborsMap);

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

        LOGGER.info("Convergence condition reached with element : " + currentElement + " (norm1 = " + currentElement.norm1() + ")");

        /* Terminate neighbors streams. */
        for (StreamObserver streamObserver : neighborsStreams) {
            streamObserver.onCompleted();
        }

        /* Terminate listener thread. */
        listenerThread.interrupt();

        /* Terminate server thread. */
        // !!! Server will be unreachable for the other workers, it might be an error !!!
        serverThread.interrupt();


    }

}
