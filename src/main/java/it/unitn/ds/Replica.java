package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.Replica.SendElectionList;
import scala.annotation.nowarn;
//import scala.collection.immutable.TreeMap;
import scala.concurrent.duration.Duration;

import java.io.*;
//import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/*
    TODOS
    // TODO: update aliveGroup based on crashes
    // TODO: find out if its better to use the ring arraylist of integer or the aliveGroup to handle crashes and which replicas are still alive
    // TODO: REMEMBER TO UPDATE ALIVEGROUP AFTER ELECTION

*/

public class Replica extends AbstractReplica {
    // Sequence number that is updated from the coordinator
    private int seqnum;

    // Store of the initialGroup and the group with all alive replicas that needs to
    // be updated as crashes go on
    private final Map<Integer, ActorRef> initialGroup;
    private TreeMap<Integer, ActorRef> aliveGroup;

    // This is the alternative to aliveGroup
    private ArrayList<Integer> ring;

    // Here i know who is the coordinator and how many replicas are in the system
    // easily
    private int coordinatorID;
    private int n;
    private int originalN;

    // I might not need it in the future, but probably its better to have it just in
    // case :)
    private boolean isCrashed;

    // timers, the first is the replica timers that after 3 seconds asks for an
    // election
    // the other is the coordinator timer where every one second it sends an
    // heartbeat
    private Cancellable watchdogTimer;
    private Cancellable coordinatorHeartbeatTimer;
    // if replica does not receive the ACK then the timer will fire and tell the
    // other hop without problems
    private Cancellable replicaElectionAwaitACKTimer;

    // Super secret data that we must register or something
    private Map<Integer, String> data;

    // Save what is the reason for the crash just for show and to keep track after
    // how many messages it should crash
    private Crash howCrash = null;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);

        // TODO: implement
        this.initialGroup = new TreeMap<>();
        this.aliveGroup = new TreeMap<>();
        this.howCrash = null;
        this.isCrashed = false;
        this.seqnum = 0;

    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return this.n;
    }

    //
    // HEARTBEAT AND ELECTION PROCESS
    //

    // Heartbeat that sends a message with sequence number (just cause, no real
    // reason) and the senderID
    public static final class Heartbeat implements Serializable {
        public final int seqnum;
        public final int senderID;

        public Heartbeat(int seqnum, int senderID) {
            this.seqnum = seqnum;
            this.senderID = senderID;
        }
    }

    // TODO: implements send election data to next hop and election ack as answer
    // TODO: Implement also the logic behind those two things
    public static final class SendElectionList implements Serializable {
        public final int initiator;
        public final TreeMap<Integer, ActorRef> aliveNodes;
        public final int seqNum;

        public SendElectionList(int initiator, TreeMap<Integer, ActorRef> aliveNodes, int seqnum) {
            this.aliveNodes = new TreeMap<>();
            this.initiator = initiator;
            this.aliveNodes.putAll(aliveNodes);
            this.seqNum = seqnum;
        }

    }

    public static final class ElectionACK implements Serializable {
    }

    /*
     * In case there is a problem with the election message that does not reach the
     * replica cause its dead
     * i send myself the reminder that something is not working
     * I also save the list of host that has been sent so that i can modify the
     * message and send it directly
     * by changing the targetID and resending everything
     * 
     */
    public static final class ElectionFailed implements Serializable {
        public int targetID;
        public int seqNum;
        public SendElectionList list;

        public ElectionFailed(int targetID, int seqNum, SendElectionList list) {
            this.targetID = targetID;
            this.seqNum = seqNum;
            this.list = list;
        }
    }

    // Answer from a note to determine if it is down or no. This should be good i
    // guess
    public static final class HeartbeatAnswer implements Serializable {
    }

    // On receive of ElectionACK from next replica i must not do anything cause the
    // message was successfully received
    // thus just cancel my timer for the recovery and await the coordinator message
    // that bullies everyone
    // into accepting the new one
    public void onElectionACK(ElectionACK e) {
        this.replicaElectionAwaitACKTimer.cancel();
        this.replicaElectionAwaitACKTimer = null;

    }

    // NOTE: This is probably useless since having a message that starts the
    // election process is redundant
    public static final class StartElectionProcess implements Serializable {
    }

    public static final class EndElectionProcess implements Serializable {
        public int seqNum;
        public int newCoordinatorId;
        public TreeMap<Integer, ActorRef> newAliveGroup;

        public EndElectionProcess(int seqNum, int newCoordinatorId, TreeMap<Integer, ActorRef> newAliveGroup) {
            this.seqNum = seqNum;
            this.newCoordinatorId = newCoordinatorId;
            this.newAliveGroup = newAliveGroup;
        }
    }

    public static final class EndElectionFailed implements Serializable {
        public int targetID;
        public int seqNum;
        public EndElectionProcess list;

        public EndElectionFailed(int targetID, int seqNum, EndElectionProcess list) {
            this.targetID = targetID;
            this.seqNum = seqNum;
            this.list = list;
        }
    }

    // Gets the next id in the list of alive replicas
    private Integer nextIdInList(int currentID) {
        Integer nextId = this.aliveGroup.higherKey(currentID);

        return nextId != null ? nextId : this.aliveGroup.firstKey();

    }

    private void resetAllClocks() {
        if (this.watchdogTimer != null) {
            this.watchdogTimer.cancel();
            this.watchdogTimer = null;
        }

        if (this.coordinatorHeartbeatTimer != null) {
            this.coordinatorHeartbeatTimer.cancel();
            this.coordinatorHeartbeatTimer = null;
        }

    }

    /*
     * 
     * Initiator (lowest initiate the election)
     * The token goes up the chain, the last one is gonna be the highest bidder.
     * So the initiator which gets the full list of the election, also needs to send
     * the finish election.
     * The problem is when the initiator crashes and never sends the acknowledge, so
     * i must have a cancellable timer
     */

    public void onReceiveElectionList(SendElectionList list) {
        int nextID = -1;
        // int nextHopCounter = 1;
        if (list.initiator != this.id) {
            resetAllClocks();
            // send election start message or election
            TreeMap<Integer, ActorRef> aliveNodes = list.aliveNodes;
            aliveNodes.put(this.id, this.initialGroup.get(this.id));
            nextID = nextIdInList(this.id);
            if (nextID == -1) {
                log(String.format("REPLICA %d, ERROR: nextID not found in aliveGROUP\n", this.id));
            }
            SendElectionList message = new SendElectionList(nextID, aliveNodes, list.seqNum);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionFailed((this.id + 1) % this.originalN, this.seqnum, message),
                    getContext().getSystem().getDispatcher(),
                    getSelf());

            tell(message, this.initialGroup.get(nextID));
            // TODO: check if this solution would work using the lists
        } else {
            // send finish election message
            this.aliveGroup = list.aliveNodes;
            // TODO: remember to reinstantiate all clocks in the next message

            // Create the end election message with the new list and the new
            nextID = nextIdInList(this.id);
            EndElectionProcess finalMessage = new EndElectionProcess(list.seqNum, nextID, this.aliveGroup);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new EndElectionFailed((this.id + 1) % this.originalN, this.seqnum, finalMessage),
                    getContext().getSystem().getDispatcher(),
                    getSelf());

            tell(finalMessage, this.initialGroup.get(nextID));

        }

    }

    // TODO: need to implement all the callbacks that are required by the project
    public void onFailedACK(ElectionFailed fail) {
        /*
         * I get the failed message, so i resend it
         * 
         */

        if (this.replicaElectionAwaitACKTimer != null) {
            this.replicaElectionAwaitACKTimer.cancel();
            this.replicaElectionAwaitACKTimer = null;
        }

    }

    //
    // END HEARTBEAT AND ELECTION PROCESS
    //

    // Makes the replica not respondant to incoming messages
    // TODO: REMEMBER TO DESTROY THE TIMER(s) OTHERWISE IT WILL SEND HEARTBEAT OR DO
    // SOME WEIRD STUFF THAT WILL BE A PAIN TO DIAGNOSE
    public Receive hasCrashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        this.howCrash = how_to_crash;

        // TODO: implement
        if (how_to_crash.type == Crash.Type.Now) {
            this.isCrashed = true;
            getContext().become(hasCrashed());
        }

        // TODO: implement other types of crash inside other message receives.

    }

    @Override
    public void initSystem(InitSystem sysInit) {
        if (this.n > 0)
            return;
        // TODO: implement

        // Copy all information relevant to the system in a Map and the size of the list
        // TODO: Must be updated after heartbeats fail when nodes crashes
        this.initialGroup.putAll(sysInit.group);
        this.aliveGroup.putAll(sysInit.group);
        this.n = this.initialGroup.size();
        this.originalN = this.n;

        // initialization of the ring, probably remove this in order to use only the
        // alivegroup
        this.ring.addAll(this.initialGroup.keySet());

        this.coordinatorID = sysInit.coordinator_id;
        seqnum = 0;

        if (this.coordinatorID == this.id)
            ;
        // TODO: Start coordinator heartbeat

        // TODO: start replica heartbeat to check for other replicas.

    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
