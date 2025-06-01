package org.example.sharedmemory.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.communication.PerfectLink;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.domain.EpochState;
import org.example.sharedmemory.util.Util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EpochConsensus extends Abstraction {

    private final int ets;
    private final ProtoPayload.ProcessId leader;

    private EpochState state;
    private ProtoPayload.Value tmpVal;
    private final Map<ProtoPayload.ProcessId, EpochState> states;
    private int accepted;
    private boolean halted;

    public EpochConsensus(String abstractionId, org.example.sharedmemory.domain.Process process, int ets, ProtoPayload.ProcessId leader, EpochState state) {
        super(abstractionId, process);
        this.ets = ets;
        this.leader = leader;

        this.state = state;
        this.tmpVal = Util.buildUndefinedValue();
        this.states = new HashMap<>();
        this.accepted = 0;
        this.halted = false;

        process.registerAbstraction(new BEB(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        if (halted) {
            log.info("[EP] Ignoring message: {} because EP is halted.", message.getType());
            return false;
        }

        switch (message.getType()) {
            case EP_PROPOSE:
                if (leader.equals(process.getProcess())) {
                    log.info("[EP] Leader proposing value: {}", tmpVal.getV());
                    tmpVal = message.getEpPropose().getValue();
                    triggerBebBroadcastEpInternalRead();
                    return true;
                }
                return false;
            case BEB_DELIVER:
                ProtoPayload.BebDeliver bebDeliver = message.getBebDeliver();
                log.info("[EP] BEB_DELIVER received from: {}, InnerType: {}", bebDeliver.getSender().getRank(), bebDeliver.getMessage().getType());


                switch (bebDeliver.getMessage().getType()) {
                    case EP_INTERNAL_READ:
                        triggerPlSendEpochState(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_WRITE:
                        state = new EpochState(ets, bebDeliver.getMessage().getEpInternalWrite().getValue());
                        log.info("[EP] State updated to value: {} at timestamp: {}", state.getVal().getV(), ets);
                        triggerPlSendEpAccept(bebDeliver.getSender());
                        return true;
                    case EP_INTERNAL_DECIDED:
                        log.info("[EP] Received decision with value: {}", bebDeliver.getMessage().getEpInternalDecided().getValue().getV());
                        triggerEpDecide(bebDeliver.getMessage().getEpInternalDecided().getValue());
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                ProtoPayload.PlDeliver plDeliver = message.getPlDeliver();
                log.info("[EP] PL_DELIVER from: {}, InnerType: {}", plDeliver.getSender().getRank(), plDeliver.getMessage().getType());

                switch (plDeliver.getMessage().getType()) {
                    case EP_INTERNAL_STATE:
                        ProtoPayload.EpInternalState deliveredState = plDeliver.getMessage().getEpInternalState();
                        states.put(plDeliver.getSender(), new EpochState(deliveredState.getValueTimestamp(), deliveredState.getValue()));
                        log.info("[EP] Received state from {} with timestamp: {}, value: {}", plDeliver.getSender().getRank(), deliveredState.getValueTimestamp(), deliveredState.getValue().getV());
                        performStatesCheck();
                        return true;
                    case EP_INTERNAL_ACCEPT:
                        accepted++;
                        log.info("[EP] Received EP_INTERNAL_ACCEPT. Accepted count: {}", accepted);
                        performAcceptedCheck();
                        return true;
                    default:
                        return false;
                }
            case EP_ABORT:
                log.info("[EP] EP_ABORT received. Halting EP.");
                triggerEpAborted();
                halted = true;
                return true;
            default:
                return false;
        }
    }

    private void triggerBebBroadcastEpInternalRead() {
        ProtoPayload.EpInternalRead epRead = ProtoPayload.EpInternalRead
                .newBuilder()
                .build();

        ProtoPayload.Message epReadMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_INTERNAL_READ)
                .setEpInternalRead(epRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(epReadMessage);
    }

    private void triggerPlSendEpochState(ProtoPayload.ProcessId sender) {
        ProtoPayload.EpInternalState EpochState = ProtoPayload.EpInternalState
                .newBuilder()
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        ProtoPayload.Message EpochStateMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_INTERNAL_STATE)
                .setEpInternalState(EpochState)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(EpochStateMessage, sender);
    }

    private void triggerPlSendEpAccept(ProtoPayload.ProcessId sender) {
        ProtoPayload.EpInternalAccept epAccept = ProtoPayload.EpInternalAccept
                .newBuilder()
                .build();

        ProtoPayload.Message epAcceptMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_INTERNAL_ACCEPT)
                .setEpInternalAccept(epAccept)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(epAcceptMessage, sender);
    }

    private void performStatesCheck() {
        if (states.size() > process.getProcesses().size() / 2) {
            EpochState highest = getHighestState();
            if (highest.getVal().getDefined()) {
                tmpVal = highest.getVal();
            }

            states.clear();

            ProtoPayload.EpInternalWrite epWrite = ProtoPayload.EpInternalWrite
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            ProtoPayload.Message epWriteMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(epWrite)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epWriteMessage);
        }
    }

    private void performAcceptedCheck() {
        if (accepted > process.getProcesses().size() / 2) {
            accepted = 0;

            ProtoPayload.EpInternalDecided epDecided = ProtoPayload.EpInternalDecided
                    .newBuilder()
                    .setValue(tmpVal)
                    .build();

            ProtoPayload.Message epDecidedMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.EP_INTERNAL_DECIDED)
                    .setEpInternalDecided(epDecided)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(epDecidedMessage);
        }
    }

    private void triggerEpDecide(ProtoPayload.Value value) {
        ProtoPayload.EpDecide epDecide = ProtoPayload.EpDecide
                .newBuilder()
                .setEts(ets)
                .setValue(value)
                .build();

        ProtoPayload.Message epDecideMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_DECIDE)
                .setEpDecide(epDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epDecideMessage);
    }

    private void triggerEpAborted() {
        ProtoPayload.EpAborted epAborted = ProtoPayload.EpAborted
                .newBuilder()
                .setEts(ets)
                .setValueTimestamp(state.getValTimestamp())
                .setValue(state.getVal())
                .build();

        ProtoPayload.Message epAbortedMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_ABORTED)
                .setEpAborted(epAborted)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortedMessage);
    }

    private void triggerBebBroadcast(ProtoPayload.Message message) {
        ProtoPayload.BebBroadcast bebBroadcast = ProtoPayload.BebBroadcast
                .newBuilder()
                .setMessage(message)
                .build();

        ProtoPayload.Message bebBroadcastMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.BEB))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }

    private void triggerPlSend(ProtoPayload.Message message, ProtoPayload.ProcessId destination) {
        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(destination)
                .setMessage(message)
                .build();

        ProtoPayload.Message plSendMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(plSendMessage);
    }

    private EpochState getHighestState() {
        return states.values().stream()
                .max(Comparator.comparing(EpochState::getValTimestamp))
                .orElse(new EpochState(0, Util.buildUndefinedValue()));
    }
}
