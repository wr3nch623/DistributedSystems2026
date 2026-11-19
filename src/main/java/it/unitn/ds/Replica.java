package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.annotation.nowarn;
import scala.concurrent.duration.Duration;


import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {
    // Sequence number that is updated from the coordinator
    private int seqnum;

    // Store of the initialGroup and the group with all alive replicas that needs to be updated as crashes go on
    // TODO: update aliveGroup based on crashes
    // TODO: find out if its better to use the ring arraylist of integer or the aliveGroup to handle crashes and which replicas are still alive
    // TODO: REMEMBER TO UPDATE ALIVEGROUP AFTER ELECTION
    private final Map<Integer, ActorRef> initialGroup;
    private Map<Integer, ActorRef> aliveGroup;



    // This is the alternative to aliveGroup
    private ArrayList<Integer> ring;

    // Here i know who is the coordinator and how many replicas are in the system easily
    private int coordinatorID;
    private int n;
    private int originalN;

    // I might not need it in the future, but probably its better to have it just in case :)
    private boolean isCrashed;

    // timers, the first is the replica timers that after 3 seconds asks for an election
    // the other is the coordinator timer where every one second it sends an heartbeat
    private Cancellable watchdogTimer;
    private Cancellable coordinatorHeartbeatTimer;
    // if replica does not receive the ACK then the timer will fire and tell the other hop without problems
    private Cancellable replicaElectionAwaitACKTimer;


    // Super secret data that we must register or something
    // TODO: see if having another index with the write clock (SeqNum) is necessary for replicas that crash but come online again
    // or a valid/invalid data something idk. Probably not cause replicas should not come online again, but am too lazy to cancel comment
    private Map<Integer, String> data;

    // Save what is the reason for the crash just for show and to keep track after how many messages it should crash
    private Crash howCrash = null;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        
        // TODO: implement
        this.initialGroup = new TreeMap<Integer, ActorRef>();
        this.howCrash = null;
        this.isCrashed = false;
        this.seqnum = 0;

    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return this.n;
    }

    // Heartbeat that sends a message with sequence number (just cause, no real reason) and the senderID
    public static final class Heartbeat implements Serializable {
        public final int seqnum;
        public final int senderID;

        public Heartbeat(int seqnum, int senderID){
            this.seqnum = seqnum;
            this.senderID = senderID;
        }
    }

    // TODO: implements send election data to next hop and election ack as answer
    // TODO: Implement also the logic behind those two things
    public static final class SendElectionList implements Serializable{
        public final int initiator;
        public final List<Integer> aliveNodes;
        public final int seqNum;

        public SendElectionList(int initiator, List<Integer> aliveNodes, int seqnum){
            this.initiator = initiator;
            this.aliveNodes = aliveNodes;
            this.seqNum = seqnum;
        }

    }
    public static final class ElectionACK implements Serializable {}

    // On receive of ElectionACK from next replica i must not do anything cause the message was successfully received
    // thus just cancel my timer for the recovery and await the coordinator message that bullies everyone
    // into accepting the new one
    public void onElectionACK(ElectionACK e){
        this.replicaElectionAwaitACKTimer = null;
    }
    
    /*
        In case there is a problem with the election message that does not reach the replica cause its dead
        i send myself the reminder that something is not working
        I also save the list of host that has been sent so that i can modify the message and send it directly
        by changing the targetID and resending everything
    
    */
    public static final class ElectionFailed implements Serializable {
        public int targetID;
        public int seqNum;
        public SendElectionList list;

        public ElectionFailed(int targetID, int seqNum, SendElectionList list){
            this.targetID = targetID;
            this.seqNum = seqNum;
            this.list = list;
        }
    }
    
    // NOTE: This is probably useless since having a message that starts the election process is redundant
    public static final class StartElectionProcess implements Serializable {}

    // TODO: remember to check for aliveGroup instead of initialGroup
    public void onReceiveElectionList(SendElectionList list) {
        int nextHopCounter = 1;
        if(list.initiator != this.id){
            // send election start message or election 
            List<Integer> aliveNodes = list.aliveNodes;
            aliveNodes.add(this.id);
            ElectionFailed message = new ElectionFailed(0, 0, null);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionFailed((this.id + 1) % this.originalN), this.seqnum, message),
                getContext().getSystem().getDispatcher(),
                getSelf());
                tell(message, aliveGroup.get((this.id + 1)));
                // TODO: Fix this, since i will remove nodes i wanna get the next node that is in the list without errors
                // probably ditch the list and use the circular map or some shit 
            }
        else{
            // send finish election message
        }

        /*
        
            Initiator (lowest initiate the election)
            The token goes up the chain, the last one is gonna be the highest bidder.
            So the initiator which gets the full list of the election, also needs to send the finish election. 
            The problem is when the initiator crashes and never sends the acknowledge, so i must have a cancellable timer
        */
    }

    // Answer from a note to determine if it is down or no. This should be good i guess
    public static final class HeartbeatAnswer implements Serializable {}

    // Makes the replica not respondant to incoming messages
    // TODO: REMEMBER TO DESTROY THE TIMER(s) OTHERWISE IT WILL SEND HEARTBEAT OR DO SOME WEIRD STUFF THAT WILL BE A PAIN TO DIAGNOSE
    public Receive hasCrashed() {
      return receiveBuilder()
              .matchAny(msg -> {})
              .build();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        this.howCrash = how_to_crash;

        // TODO: implement
        if (how_to_crash.type == Crash.Type.Now){
            this.isCrashed = true;
            getContext().become(hasCrashed());
        }

        // TODO: implement other types of crash inside other message receives.

    }


    @Override
    public void initSystem(InitSystem sysInit) {
        if(this.n > 0) return;
        // TODO: implement

        // Copy all information relevant to the system in a Map and the size of the list
        // TODO: Must be updated after heartbeats fail when nodes crashes
        this.initialGroup.putAll(sysInit.group);
        this.aliveGroup.putAll(sysInit.group);
        this.n = this.initialGroup.size();
        this.originalN = this.n; 

        // initialization of the ring, probably remove this in order to use only the alivegroup
        this.ring.addAll(this.initialGroup.keySet());

        this.coordinatorID = sysInit.coordinator_id;
        seqnum = 0;

        if(this.coordinatorID == this.id);
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
