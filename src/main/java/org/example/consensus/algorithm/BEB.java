package org.example.consensus.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.consensus.communication.PerfectLink;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.domain.AbstractionType;
import org.example.consensus.domain.Process;
import org.example.consensus.util.Util;

@Slf4j
public class BEB extends Abstraction {
    public BEB(String abstractionId, Process process) {
        super(abstractionId, process);
        registerChild(AbstractionType.PL, PerfectLink::new);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        log.info("Handling message - type: {}, from: {}, to: {}, hashCode: {}",
                message.getType(), message.getFromAbstractionId(),
                message.getToAbstractionId(), message.hashCode());

        return switch (message.getType()) {
            case BEB_BROADCAST -> {
                log.info("Received BEB_BROADCAST from: {}", message.getFromAbstractionId());
                handleBebBroadcast(message.getBebBroadcast());
                yield true;
            }
            case PL_DELIVER -> {
                log.info("Received PL_DELIVER from: {}", message.getPlDeliver().getSender().getRank());
                triggerBebDeliver(message.getPlDeliver().getMessage(), message.getPlDeliver().getSender());
                yield true;
            }
            default -> {
                log.warn("Unhandled message type: {}", message.getType());
                yield false;
            }
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