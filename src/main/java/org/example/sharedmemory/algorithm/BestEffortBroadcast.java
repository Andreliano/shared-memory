package org.example.sharedmemory.algorithm;

import org.example.sharedmemory.communication.PerfectLink;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.domain.Process;
import org.example.sharedmemory.util.Util;

public class BestEffortBroadcast extends Abstraction {
    public BestEffortBroadcast(String abstractionId, Process process) {
        super(abstractionId, process);
        registerChild(AbstractionType.PL, PerfectLink::new);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        return switch (message.getType()) {
            case BEB_BROADCAST -> {
                handleBebBroadcast(message.getBebBroadcast());
                yield true;
            }
            case PL_DELIVER -> {
                triggerBebDeliver(message.getPlDeliver().getMessage(), message.getPlDeliver().getSender());
                yield true;
            }
            default -> false;
        };
    }

    private void handleBebBroadcast(ProtoPayload.BebBroadcast bebBroadcast) {
        process.getProcesses().forEach(p -> {
            ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                    .newBuilder()
                    .setDestination(p)
                    .setMessage(bebBroadcast.getMessage())
                    .build();

            var plSendMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.PL_SEND)
                    .setPlSend(plSend)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(plSendMessage);
        });
    }

    private void triggerBebDeliver(ProtoPayload.Message appValueMessage, ProtoPayload.ProcessId sender) {
        var bebDeliver = ProtoPayload.BebDeliver
                .newBuilder()
                .setMessage(appValueMessage)
                .setSender(sender)
                .build();

        var bebDeliverMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.BEB_DELIVER)
                .setBebDeliver(bebDeliver)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebDeliverMessage);
    }

    private void registerChild(AbstractionType type, ChildFactory factory) {
        String childId = Util.getChildAbstractionId(abstractionId, type);
        process.registerAbstraction(factory.create(childId, process));
    }

    @FunctionalInterface
    private interface ChildFactory {
        Abstraction create(String abstractionId, Process process);
    }
}