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
    ////track variables /////////////////////
    // Sequence number that is updated from the coordinator
    private int seqnum;

    // Store of the initialGroup and the group with all alive replicas that needs to
    // be updated as crashes go on
    private final Map<Integer, ActorRef> initialGroup;
    private TreeMap<Integer, ActorRef> aliveGroup;

    // This is the alternative to aliveGroup
    private ArrayList<Integer> ring = new ArrayList<>(); //should this be initialized?

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


    //////////////////////////////////////////////
    
    /////// BROADCAST STATE (replica s memory here) /////////////////////////////////

    private final int[] positions = new int[POSITIONS_LIST_LENGTH];     // the actual data 
    private int epoch = 0;          //coordinator epoch, incremented on each new coordinator
    private int nextSeq = 0;        // ONLY used by coordinator, next sequence number to assign to the next update
    private int nextToCommit = 0;   // ONLY used by coordinator, next sequence number to commit (i.e. send WriteOk to all replicas)
    private final TreeMap<UpdateId, Update> pending = new TreeMap<>(); // received, not yet applied pending proposal for updates
    private final TreeMap<UpdateId, Update> history = new TreeMap<>(); // applied updates, USEFUL FOR ELECTION
    private final Map<UpdateId, Integer> ackCount = new HashMap<>();   // ONLY coordinator, ack counter for each update proposal

    /////////////////////////////////////////////////////////////////////////////////////////////



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
        return this.aliveGroup.size();
    }

    //
    // HEARTBEAT AND ELECTION PROCESS ///////////////////////////////////////////////////////////////////////////
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

    public static final class FailHeartbeat implements Serializable {
    }

    public void onHeartbeat(Heartbeat a) {
        // need to reset heartbeat timer

        resetWatchdogTimer();
        // TODO: add reinitialization of heartbeat timer
        this.watchdogTimer = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(4L * getMaxLatency() + 8L * this.id * getMaxLatency(), TimeUnit.MILLISECONDS),
                getSelf(),
                new FailHeartbeat(),
                getContext().getSystem().getDispatcher(),
                getSelf());
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

    void startElectionProcesS(FailHeartbeat fail) {
        int nextID = -1;

        nextID = nextAliveIdInList(this.id);
        int transactionID = (int)Math.random() * 503;
        TreeMap<Integer, ActorRef> aliveNodes = new TreeMap<>();
        aliveNodes.put(this.id, this.initialGroup.get(this.id));

        SendElectionList message = new SendElectionList(nextID, aliveNodes, transactionID);
        this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionFailed(nextID, this.seqnum, message),
                getContext().getSystem().getDispatcher(),
                getSelf());

        tell(message, this.initialGroup.get(nextID));
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
            nextID = nextAliveIdInList(this.id);
            if (nextID == -1) {
                log(String.format("REPLICA %d, ERROR: nextID not found in aliveGROUP\n", this.id));
            }
            SendElectionList message = new SendElectionList(nextID, aliveNodes, list.seqNum);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionFailed(nextID, this.seqnum, message),
                    getContext().getSystem().getDispatcher(),
                    getSelf());

            tell(message, this.initialGroup.get(nextID));
            // TODO: check if this solution would work using the lists
        } else {
            // send finish election message
            this.aliveGroup = list.aliveNodes;
            // TODO: remember to reinstantiate all clocks in the next message

            // Create the end election message with the new list and the new
            nextID = nextAliveIdInList(this.id);
            EndElectionProcess finalMessage = new EndElectionProcess(list.seqNum, this.id, nextID, this.aliveGroup);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new EndElectionFailed(nextID, this.seqnum, finalMessage),
                    getContext().getSystem().getDispatcher(),
                    getSelf());

            tell(finalMessage, this.initialGroup.get(nextID));

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

    // TODO: need to implement all the callbacks that are required by the project
    public void onFailedACK(ElectionFailed fail) {
        /*
         * I get the failed message, so i resend it
         * 
         */
        int nextID = -1;

        resetReplicaACKTimer();

        // Create the end election message with the new list and the new
        nextID = nextAliveIdInList(fail.targetID);

        this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionFailed(nextID, this.seqnum, fail.list),
                getContext().getSystem().getDispatcher(),
                getSelf());

        tell(fail.list, this.initialGroup.get(nextID));

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
        resetReplicaACKTimer();

    }

    public static final class EndElectionProcess implements Serializable {
        public int seqNum;
        public int initiator;
        public int newCoordinatorId;
        public TreeMap<Integer, ActorRef> newAliveGroup;

        public EndElectionProcess(int seqNum, int initiator, int newCoordinatorId,
                TreeMap<Integer, ActorRef> newAliveGroup) {
            this.seqNum = seqNum;
            this.newCoordinatorId = newCoordinatorId;
            this.newAliveGroup = newAliveGroup;
            this.initiator = initiator;
        }
    }

    public void onEndElectionProcess(EndElectionProcess p) {
        int nextID = -1;
        if (p.initiator != this.id) {
            this.aliveGroup = p.newAliveGroup;
            this.coordinatorID = p.newCoordinatorId;

            EndElectionProcess newMessage = new EndElectionProcess(p.seqNum, p.initiator, p.newCoordinatorId,
                    p.newAliveGroup);

            nextID = nextAliveIdInList(this.id);
            this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new EndElectionFailed(nextID, this.seqnum, newMessage),
                    getContext().getSystem().getDispatcher(),
                    getSelf());

            tell(newMessage, this.initialGroup.get(nextID));

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

    public void onFailedEndElectionProcess(EndElectionFailed fail) {
        int nextID = -1;

        resetReplicaACKTimer();

        nextID = nextAliveIdInList(fail.targetID);

        EndElectionProcess newMessage = new EndElectionProcess(fail.list.seqNum, fail.list.initiator,
                fail.list.newCoordinatorId,
                fail.list.newAliveGroup);

        this.replicaElectionAwaitACKTimer = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
                getSelf(),
                new EndElectionFailed(nextID, this.seqnum, newMessage),
                getContext().getSystem().getDispatcher(),
                getSelf());

        tell(newMessage, this.initialGroup.get(nextID));

    }

    // Gets the next id in the list of alive replicas
    private Integer nextAliveIdInList(int currentID) {
        Integer nextId = this.aliveGroup.higherKey(currentID);

        return nextId != null ? nextId : this.aliveGroup.firstKey();

    }

    private void resetCoordinatorHearbeatTimer() {
        if (this.coordinatorHeartbeatTimer != null) {
            this.coordinatorHeartbeatTimer.cancel();
            this.coordinatorHeartbeatTimer = null;
        }
    }

    private void resetWatchdogTimer() {
        if (this.watchdogTimer != null) {
            this.watchdogTimer.cancel();
            this.watchdogTimer = null;
        }
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

        if (this.replicaElectionAwaitACKTimer != null) {
            this.coordinatorHeartbeatTimer.cancel();
            this.coordinatorHeartbeatTimer = null;
        }

    }

    void resetReplicaACKTimer() {
        if (this.replicaElectionAwaitACKTimer != null) {
            this.replicaElectionAwaitACKTimer.cancel();
            this.replicaElectionAwaitACKTimer = null;
        }
    }

    //
    // END HEARTBEAT AND ELECTION PROCESS ////////////////////////////////////////////////////////////////////////////////
    //

    // Makes the replica not respondant to incoming messages
    // TODO: REMEMBER TO DESTROY THE TIMER(s) OTHERWISE IT WILL SEND HEARTBEAT OR DO
    // SOME WEIRD STUFF THAT WILL BE A PAIN TO DIAGNOSE






    /////////BROADCAST PROTOCOL (reads, writes, UPDATE / ACK / WRITEOK)/////////////////////////////////////

    /////Messages (immutable objects)///////
    
    public static final class UpdateId implements Serializable, Comparable<UpdateId> { //Identifies and update as epoque,seq
        public final int epoch;
        public final int seq;

        public UpdateId(int epoch, int seq) {
            this.epoch = epoch;
            this.seq = seq;
        }

        @Override 
        public int compareTo(UpdateId o) {
            if (epoch != o.epoch) {
                return Integer.compare(epoch, o.epoch);
            }
            return Integer.compare(seq, o.seq);
        }

        @Override 
        public boolean equals (Object o) {
            if(!(o instanceof UpdateId))
                return false;
            UpdateId u = (UpdateId) o;
            return u.epoch == epoch && u.seq == seq;
        }

        @Override 
        public int hashCode() {
            return Objects.hash(epoch, seq);
        }

        @Override 
        public String toString() {
            return epoch + ":" + seq;
        }
    }

    //Replica to coordinator (a client asked me to write)
    public static final class ForwardedWrite implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef client;
        public final int originReplica;

        public ForwardedWrite(int index, int value, ActorRef client, int originReplica) {
            this.index = index;
            this.value = value;
            this.client = client;
            this.originReplica = originReplica;
        }
    }

    //Coordinator to ALL replicas
    public static final class Update implements Serializable {
        public final UpdateId id;
        public final int index;
        public final int value;
        public final ActorRef client;
        public final int originReplica;

        public Update(UpdateId id, int index, int value, ActorRef client, int originReplica) {
            this.id = id;
            this.index = index;
            this.value = value;
            this.client = client;
            this.originReplica = originReplica;
        }
    }

    //Replica to coordinator
    public static final class Ack implements Serializable {
        public final UpdateId id;

        public Ack(UpdateId id) {
            this.id = id;
        }
    }

    //Coordinator to ALL replicas
    public static final class WriteOk implements Serializable {
        public final UpdateId id;

        public WriteOk(UpdateId id) {
            this.id = id;
        }
    }   

///////Helpers//////////
    
    private int quorum() {
        return this.originalN / 2 + 1; //originalN is the number of replicas in the system, NOT THE ONES ALIVE, so that we can reach a quorum even if some replicas are down
    }

    private boolean isCoordinator() {  //CHECK if this replica is the coordinator, used to know if it should start the broadcast or just forward the write request to the coordinator
        return this.id == this.coordinatorID;
    }

    //Client requests///////////////////

    //Read: answered immediately from the local copy
    private void onRead(AbstractClient.ReadRequest r) {
        boolean ok = r.index >= 0 && r.index < positions.length; //From local memory
        Integer val = ok ? Integer.valueOf(positions[r.index]) : null;
        tell(new AbstractClient.ReadResult(ok, r.index, val, this.id), getSender());    //DANGER HERE: a read might not see a concurrent write that hasn't been applied yet 
    }

    //Write: the coordinator starts the broadcast, the others forward the request
    private void onWrite(AbstractClient.WriteRequest w) {
        if (isCoordinator()) {
            startBroadcast(w.index, w.value, getSender(), this.id);
        } else {
            tell(new ForwardedWrite(w.index, w.value, getSender(), this.id),
                    this.initialGroup.get(this.coordinatorID));
            // TODO: start the timeout "coordinator did not broadcast the update"
        }
    }

    private void onForwardedWrite(ForwardedWrite f) {   //When the write request is forwarded to the coordinator, it starts the broadcast
        if (isCoordinator()) {
            startBroadcast(f.index, f.value, f.client, f.originReplica);
        }
    }

    //// Two-phase broadcast /////////////

    // PHASE 1 (coordinator): send UPDATE to everyone
    private void startBroadcast(int index, int value, ActorRef client, int origin) {
        UpdateId uid = new UpdateId(this.epoch, this.nextSeq++);    //new updateID 
        Update u = new Update(uid, index, value, client, origin);   //new update message

        pending.put(uid, u);    //hold the update
        ackCount.put(uid, 1); // the coordinator counts itself in the quorum

        for (Map.Entry<Integer, ActorRef> e : aliveGroup.entrySet()) {
            if (e.getKey() != this.id) {
                tell(u, e.getValue());  //sent message to all alive replicas except itself
            }
        }
    }



    //PHASE 1 (replica): store the update as pending and ACK the coordinator
    private void onUpdate(Update u) {
        pending.put(u.id, u);   //store update
        tell(new Ack(u.id), this.initialGroup.get(this.coordinatorID)); //ack with only updateID
        //TODO: start the timeout "WRITEOK did not arrive"
    }



    //PHASE 2 (coordinator): when a quorum is reached, send WRITEOK in sequence order

    private void onAck(Ack a) {
        if (!pending.containsKey(a.id)) //TO AVOID applying an update that has already been applied (e.g. if a replica crashed and restarted, it might send an ACK for an update that has already been committed)
            return; // already committed: ignore late ACKs
        ackCount.merge(a.id, 1, Integer::sum);  //MERGE TO keep duplicates harmless

        while (true) { //DISGUSTING I KNOW, used when there are more pending updates to process all in one go, in case a quorum is reached for more than one update
            UpdateId next = new UpdateId(this.epoch, this.nextToCommit);
            Integer c = ackCount.get(next);
            if (c == null || c < quorum())  //STOP if no entry, NOT YET proposed or already committed
                break;

            Update u = pending.get(next);
            for (Map.Entry<Integer, ActorRef> e : aliveGroup.entrySet()) {  //ask for WRITEOK
                if (e.getKey() != this.id) {
                    tell(new WriteOk(next), e.getValue());
                }
            }
            applyUpdate(u); //LOCALLY
            ackCount.remove(next);
            this.nextToCommit++;
        }
    }

    //PHASE 2 (replica): apply the update
    private void onWriteOk(WriteOk w) {
        Update u = pending.get(w.id);
        if (u != null) {
            applyUpdate(u);
        }
        // TODO: cancel the "WRITEOK did not arrive" timeout for w.id
    }

    //DATA GETS UPDATED HERE////
    private void applyUpdate(Update u) {
        if (history.containsKey(u.id))
            return; // NEVER apply the same update twice

        pending.remove(u.id);
        positions[u.index] = u.value;
        history.put(u.id, u);

        log("applied update " + u.id + " (" + u.index + ", " + u.value + ")");
        callbackOnUpdateApplied(u.index, u.value);

        //Burocratic part: replica that received the client request tells the client
        if (u.originReplica == this.id && u.client != null) {
            tell(new AbstractClient.WriteResult(true, u.index, u.value, this.id), u.client);
        }
    }


    ///////END BROADCAST PROTOCOL/////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////


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

    // public void sendMessageAndACK(int targetID, Object targetMessage, Object
    // errorMessage){

    // resetReplicaACKTimer();;

    // this.replicaElectionAwaitACKTimer =
    // getContext().getSystem().scheduler().scheduleOnce(
    // Duration.create(4L * getMaxLatency(), TimeUnit.MILLISECONDS),
    // getSelf(),
    // errorMessage,
    // getContext().getSystem().getDispatcher(),
    // getSelf());

    // tell((Serializable) targetMessage, this.initialGroup.get(targetID));
    // }

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

        if (this.coordinatorID == this.id) {
            // TODO: Start coordinator heartbeat

        }

        // TODO: start replica heartbeat to check for other replicas.

    }

    @Override
    public final Receive createReceive() {  //Fancy switch case that handles all the messages
        return createBaseReceiveBuilder()
                // TODO[DONE] add your message handlers here .match(, )
                //////broadcast protocol //////
                .match(AbstractClient.ReadRequest.class, this::onRead)
                .match(AbstractClient.WriteRequest.class, this::onWrite)
                .match(ForwardedWrite.class, this::onForwardedWrite)
                .match(Update.class, this::onUpdate)
                .match(Ack.class, this::onAck)
                .match(WriteOk.class, this::onWriteOk)
                // heartbeat and elections
                .match(ElectionACK.class, this::onElectionACK)
                .match(ElectionFailed.class, this::onFailedACK)
                .match(EndElectionFailed.class, this::onFailedEndElectionProcess)
                .match(SendElectionList.class, this::onReceiveElectionList)
                .match(EndElectionProcess.class, this::onEndElectionProcess)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(FailHeartbeat.class, this::startElectionProcesS)
                //end heartbeat and elections
                .build();
    }

}
