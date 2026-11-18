package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import scala.annotation.nowarn;

import java.io.*;
import java.util.*;

public class Replica extends AbstractReplica {
    // Sequence number that is updated from the coordinator
    private int seqnum;

    // Store of the initialGroup and the group with all alive replicas that needs to be updated as crashes go on
    // TODO: update aliveGroup based on crashes
    // TODO: find out if its better to use the ring arraylist of integer or the aliveGroup to handle crashes and which replicas are still alive
    private final Map<Integer, ActorRef> initialGroup;
    private Map<Integer, ActorRef> aliveGroup;

    // This is the alternative to aliveGroup
    private ArrayList<Integer> ring;

    // Here i know who is the coordinator and how many replicas are in the system easily
    private int coordinatorID;
    private int n;

    // I might not need it in the future, but probably its better to have it just in case :)
    private boolean isCrashed;


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
