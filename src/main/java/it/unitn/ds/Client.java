package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

//more imports

import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;



//////////////////////
/// 

public class Client extends AbstractClient {


    //Read and write Timers to handle timeouts, separate cause separate are also the calls
    private final Map<Integer, Cancellable> readTimers = new HashMap<>();
    private final Map<Integer, Cancellable> writeTimers = new HashMap<>();


    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    // TODO: check if this message definition is right or something
    /**public static class SendReadMessage implements Serializable{
        public final ActorRef replica;
        public final int index;

        public SendReadMessage(ActorRef replica, int index){
            this.replica = replica;
            this.index = index;
        }
    }**/
////////////////////////Send Read/Write and more ////////////////////////////////////////////////////////////////////
    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: implement
        log("requesting READ ("+ index +") to " + replica.path().name());
        replica.tell(new AbstractClient.ReadRequest(index), getSelf());

        //here remove existing timer to prevents duplicate/overlapping timeouts if a retry happens
        cancel(readTimers.remove(index));
        readTimers.put(index, getContext().system().scheduler().scheduleOnce(
                Duration.create(getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new ReadTimeout(getSelf(), replica, index),
                getContext().getDispatcher(),
                getSelf()
        )); 

    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
        log("requesting WRITE ("+ index +", "+ value +") to " + replica.path().name());
        replica.tell(new AbstractClient.WriteRequest(index, value), getSelf());

        //here remove existing timer to prevents duplicate/overlapping timeouts if a retry happens
        cancel(writeTimers.remove(index));
        writeTimers.put(index, getContext().system().scheduler().scheduleOnce(
                Duration.create(getWriteTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new WriteTimeout(getSelf(), replica, index, value),
                getContext().getDispatcher(),
                getSelf()
        )); 
    }


    //supporting methods//////////////////////
    private void cancel(Cancellable timer) {
        if (timer != null) timer.cancel(); //null when there is no entry for that index
        }
    
    private void onReadResult (ReadResult r) { //when a replica's reply arrives
        cancel(readTimers.remove(r.index)); //stop timer
        callbackOnReadResult(r); //log important callback
    }

     private void onWriteResult (WriteResult r) { //when a replica's reply arrives
        cancel(writeTimers.remove(r.index)); //stop timer
        callbackOnWriteResult(r); //log important callback
    }   

    private void onReadTimeout(ReadTimeout t) { //when the timer fires/the replica didnt answer in time
        readTimers.remove(t.index); //remove entry from the map
        callbackOnReadTimeout(t); //log important callback
    }

    private void onWriteTimeout(WriteTimeout t) { //when the timer fires/the replica didnt answer in time
        writeTimers.remove(t.index); //remove entry from the map
        callbackOnWriteTimeout(t); //log important callback
    }
    ////////////////////////////////////////


    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(ReadResult.class, this::onReadResult)
                .match(WriteResult.class, this::onWriteResult)
                .match(ReadTimeout.class, this::onReadTimeout)
                .match(WriteTimeout.class, this::onWriteTimeout)
                .build();
    }

}
