package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.Optional;

public class Client extends AbstractClient {

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
    public static class SendReadMessage implements Serializable{
        public final ActorRef replica;
        public final int index;

        public SendReadMessage(ActorRef replica, int index){
            this.replica = replica;
            this.index = index;
        }
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: implement
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
