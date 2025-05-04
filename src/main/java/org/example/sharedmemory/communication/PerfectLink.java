package org.example.sharedmemory.communication;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.algorithm.Abstraction;
import org.example.sharedmemory.domain.Process;
import org.example.sharedmemory.util.Util;

import java.util.Optional;
import java.util.UUID;

@Slf4j
public class PerfectLink extends Abstraction {
    public PerfectLink(String abstractionId, Process process) {
        super(abstractionId, process);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        return switch (message.getType()) {
            case PL_SEND -> {
                log.info("[PerfectLink] Handling PL_SEND from '{}'", message.getFromAbstractionId());
                handlePlSend(message.getPlSend(), message.getToAbstractionId());
                yield true;
            }
            case NETWORK_MESSAGE -> {
                log.info("[PerfectLink] Handling NETWORK_MESSAGE from host={} port={}",
                        message.getNetworkMessage().getSenderHost(),
                        message.getNetworkMessage().getSenderListeningPort());
                triggerPlDeliver(message.getNetworkMessage(), Util.getParentAbstractionId(message.getToAbstractionId()));
                yield true;
            }
            default -> false;
        };
    }

    private void handlePlSend(ProtoPayload.PlSend plSendMessage, String toAbstractionId) {
        ProtoPayload.ProcessId sender = process.getProcess();
        ProtoPayload.ProcessId destination = plSendMessage.getDestination();

        log.info("[PerfectLink] Sending message from {}:{} to {}:{} | Abstraction: {}",
                sender.getHost(), sender.getPort(), destination.getHost(), destination.getPort(), toAbstractionId);

        var networkMessage = ProtoPayload.NetworkMessage
                .newBuilder()
                .setSenderHost(sender.getHost())
                .setSenderListeningPort(sender.getPort())
                .setMessage(plSendMessage.getMessage())
                .build();

        var sentNetworkMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(process.getSystemId())
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(sentNetworkMessage, destination.getHost(), destination.getPort());
    }


    private void triggerPlDeliver(ProtoPayload.NetworkMessage networkMessage, String toAbstractionId) {
        Optional<ProtoPayload.ProcessId> sender = process.getProcessByHostAndPort(networkMessage.getSenderHost(), networkMessage.getSenderListeningPort());
        ProtoPayload.PlDeliver.Builder plDeliverBuilder = ProtoPayload.PlDeliver
                .newBuilder()
                .setMessage(networkMessage.getMessage());
        sender.ifPresent(plDeliverBuilder::setSender);

        ProtoPayload.PlDeliver plDeliver = plDeliverBuilder.build();

        var message = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.PL_DELIVER)
                .setPlDeliver(plDeliver)
                .setToAbstractionId(toAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        log.info("[PerfectLink] Delivering message to abstraction: {} | Message type: {}",
                toAbstractionId, networkMessage.getMessage().getType());

        process.addMessageToQueue(message);
    }
}
