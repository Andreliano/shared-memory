package org.example.consensus.algorithm;

import org.example.consensus.communication.PerfectLink;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.domain.AbstractionType;
import org.example.consensus.util.Util;

public class EpochChange extends Abstraction {

    private ProtoPayload.ProcessId trusted;
    private int lastTimestamp;
    private int timestamp;

    public EpochChange(String abstractionId, org.example.consensus.domain.Process process) {
        super(abstractionId, process);
        trusted = Util.getMaxRankedProcess(process.getProcesses());
        lastTimestamp = 0;
        timestamp = process.getProcess().getRank();

        process.registerAbstraction(new EventualLeaderDetector(Util.getChildAbstractionId(abstractionId, AbstractionType.ELD), process));
        process.registerAbstraction(new BEB(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case ELD_TRUST:
                handleEldTrust(message.getEldTrust().getProcess());
                return true;
            case BEB_DELIVER:
                ProtoPayload.BebDeliver bebDeliver = message.getBebDeliver();
                if (bebDeliver.getMessage().getType() == ProtoPayload.Message.Type.EC_INTERNAL_NEW_EPOCH) {
                    handleBebDeliverNewEpoch(bebDeliver.getSender(), bebDeliver.getMessage().getEcInternalNewEpoch().getTimestamp());
                    return true;
                }
                return false;
            case PL_DELIVER:
                ProtoPayload.PlDeliver plDeliver = message.getPlDeliver();
                if (plDeliver.getMessage().getType() == ProtoPayload.Message.Type.EC_INTERNAL_NACK) {
                    handleNack();
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private void handleEldTrust(ProtoPayload.ProcessId p) {
        trusted = p;
        if (p.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void handleBebDeliverNewEpoch(ProtoPayload.ProcessId sender, int newTimestamp) {
        if (sender.equals(trusted) && newTimestamp > lastTimestamp) {
            lastTimestamp = newTimestamp;
            ProtoPayload.EcStartEpoch startEpoch = ProtoPayload.EcStartEpoch
                    .newBuilder()
                    .setNewLeader(sender)
                    .setNewTimestamp(newTimestamp)
                    .build();

            ProtoPayload.Message startEpochMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.EC_START_EPOCH)
                    .setEcStartEpoch(startEpoch)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(startEpochMessage);
        } else {
            ProtoPayload.EcInternalNack ecNack = ProtoPayload.EcInternalNack
                    .newBuilder()
                    .build();

            ProtoPayload.Message nackMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.EC_INTERNAL_NACK)
                    .setEcInternalNack(ecNack)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                    .newBuilder()
                    .setDestination(sender)
                    .setMessage(nackMessage)
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
    }

    private void handleNack() {
        if (trusted.equals(process.getProcess())) {
            timestamp += process.getProcesses().size();
            triggerBebBroadcastNewEpoch();
        }
    }

    private void triggerBebBroadcastNewEpoch() {
        ProtoPayload.EcInternalNewEpoch newEpoch = ProtoPayload.EcInternalNewEpoch
                .newBuilder()
                .setTimestamp(timestamp)
                .build();

        ProtoPayload.Message newEpochMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EC_INTERNAL_NEW_EPOCH)
                .setEcInternalNewEpoch(newEpoch)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.BebBroadcast bebBroadcast = ProtoPayload.BebBroadcast
                .newBuilder()
                .setMessage(newEpochMessage)
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
}
