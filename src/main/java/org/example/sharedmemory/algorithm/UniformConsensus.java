package org.example.sharedmemory.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.domain.EpochState;
import org.example.sharedmemory.util.Util;

@Slf4j
public class UniformConsensus extends Abstraction {

    private ProtoPayload.Value val;
    private boolean proposed;
    private boolean decided;
    private int epochTimestamp;
    private ProtoPayload.ProcessId leader;
    private int newEpochTimestamp;
    private ProtoPayload.ProcessId newLeader;

    public UniformConsensus(String abstractionId, org.example.sharedmemory.domain.Process process) {
        super(abstractionId, process);

        val = Util.buildUndefinedValue();
        proposed = false;
        decided = false;

        ProtoPayload.ProcessId leader0 = Util.getMaxRankedProcess(process.getProcesses());
        epochTimestamp = 0;
        leader = leader0;
        newEpochTimestamp = 0;
        newLeader = null;

        process.registerAbstraction(new EpochChange(Util.getChildAbstractionId(abstractionId, AbstractionType.EC), process));
        process.registerAbstraction(new EpochConsensus(Util.getNamedAbstractionId(abstractionId, AbstractionType.EP, "0"), process,
                0, leader0, new EpochState(0, Util.buildUndefinedValue())));

        log.info("[{}] Initialized with leader {}", abstractionId, leader0.getRank());
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case UC_PROPOSE:
                val = message.getUcPropose().getValue();
                log.info("[{}] Received UC_PROPOSE with value: {}", abstractionId, val.getV());
                performCheck();
                return true;
            case EC_START_EPOCH:
                newEpochTimestamp = message.getEcStartEpoch().getNewTimestamp();
                newLeader = message.getEcStartEpoch().getNewLeader();
                log.info("[{}] EC_START_EPOCH: newts={}, newLeader={}", abstractionId, newEpochTimestamp, newLeader.getRank());
                triggerEpEtsAbort();
                return true;
            case EP_ABORTED:
                if (message.getEpAborted().getEts() == epochTimestamp) {
                    log.info("[{}] EP_ABORTED for ets={}, switching to newts={}, newLeader={}", abstractionId, epochTimestamp, newEpochTimestamp, newLeader.getRank());
                    epochTimestamp = newEpochTimestamp;
                    leader = newLeader;
                    proposed = false;
                    process.registerAbstraction(new EpochConsensus(Util.getNamedAbstractionId(abstractionId, AbstractionType.EP, Integer.toString(epochTimestamp)), process,
                            epochTimestamp, leader, new EpochState(message.getEpAborted().getValueTimestamp(), message.getEpAborted().getValue())));
                    performCheck();
                    return true;
                }
                log.info("[{}] Ignored EP_ABORTED for ets={}, current ets={}", abstractionId, message.getEpAborted().getEts(), epochTimestamp);
                return false;
            case EP_DECIDE:
                if (message.getEpDecide().getEts() == epochTimestamp) {
                    if (!decided) {
                        decided = true;
                        log.info("[{}] EP_DECIDE received. Deciding value: {}", abstractionId, message.getEpDecide().getValue().getV());
                        triggerUcDecide(message.getEpDecide().getValue());
                    }
                    return true;
                }
                log.info("[{}] Ignored EP_DECIDE for ets={}, current ets={}", abstractionId, message.getEpDecide().getEts(), epochTimestamp);
                return false;
            default:
                return false;
        }
    }

    private void triggerEpEtsAbort() {
        log.info("[{}] Triggering EP_ABORT for ets={}", abstractionId, epochTimestamp);

        ProtoPayload.EpAbort epAbort = ProtoPayload.EpAbort
                .newBuilder()
                .build();

        ProtoPayload.Message epAbortMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EP_ABORT)
                .setEpAbort(epAbort)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(epochTimestamp)))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(epAbortMessage);
    }

    private void performCheck() {
        log.info("[{}] performCheck called. Leader: {}, self: {}, val defined: {}, proposed: {}", abstractionId, leader.getRank(), process.getProcess().getRank(), val.getDefined(), proposed);

        if (leader.equals(process.getProcess()) && val.getDefined() && !proposed) {
            proposed = true;
            log.info("[{}] I am the leader and proposing value: {} in epoch: {}", abstractionId, val.getV(), epochTimestamp);

            ProtoPayload.EpPropose epPropose = ProtoPayload.EpPropose
                    .newBuilder()
                    .setValue(val)
                    .build();

            ProtoPayload.Message epProposeMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.EP_PROPOSE)
                    .setEpPropose(epPropose)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(Util.getNamedAbstractionId(this.abstractionId, AbstractionType.EP, Integer.toString(epochTimestamp)))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(epProposeMessage);
        }
    }

    private void triggerUcDecide(ProtoPayload.Value value) {
        log.info("[{}] Broadcasting UC_DECIDE with value: {}", abstractionId, value.getV());

        ProtoPayload.UcDecide ucDecide = ProtoPayload.UcDecide
                .newBuilder()
                .setValue(value)
                .build();

        ProtoPayload.Message ucDecideMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.UC_DECIDE)
                .setUcDecide(ucDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(ucDecideMessage);
    }
}
