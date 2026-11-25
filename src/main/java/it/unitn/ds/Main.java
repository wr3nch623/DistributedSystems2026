package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;
//more imports
import java.util.Optional;

public class Main {

    public static void main(String[] args) throws InterruptedException {   //throws InterruptedException because of Thread.sleep() calls
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 4;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // TODO: Create your clients
        
        // TODO: Implement your main logic///////////////////////////////////////////////

        ActorRef client = system.actorOf(Client.props(1000,  2000, Optional.of(replicas.get(1))), "client_1"); //(x,y, ecc) x is timeout for read answer and y for write answer

        Thread.sleep(500); //waiting for InitSystem to be processed by all replicas
        client.tell(new AbstractClient.WriteRequest(0,42), ActorRef.noSender());
        Thread.sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        Thread.sleep(500);


        //////////////////////////////////////////////////////////////////////////////////

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
