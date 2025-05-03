package org.example.sharedmemory.domain;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.algorithm.Abstraction;
import org.example.sharedmemory.algorithm.Application;
import org.example.sharedmemory.algorithm.NNAtomicRegister;
import org.example.sharedmemory.communication.MessageReceiver;
import org.example.sharedmemory.communication.MessageSender;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.util.Util;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Slf4j
public class Process implements Runnable {

    private ProtoPayload.ProcessId process;

    private final ProtoPayload.ProcessId hub;

    private List<ProtoPayload.ProcessId> processes;

    private String systemId;

    private final BlockingQueue<ProtoPayload.Message> messageQueue;

    private final Map<String, Abstraction> abstractions;

    public Process(ProtoPayload.ProcessId process, ProtoPayload.ProcessId hub) {
        this.process = process;
        this.hub = hub;
        messageQueue = new LinkedBlockingQueue<>();
        abstractions = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        log.info("Running process {}-{}", process.getOwner(), process.getIndex());

        registerAbstraction(new Application(AbstractionType.APP.getKey(), this));

        registerToTheHub();

        String processName = String.format("%s-%d", process.getOwner(), process.getIndex());
        MessageReceiver messageReceiver = new MessageReceiver(process.getPort(), messageQueue);
        Thread messageReceiverThread = new Thread(messageReceiver, processName);
        messageReceiverThread.start();

        while (true) {
            ProtoPayload.Message message = null;
            try {
                message = messageQueue.take();
            } catch (InterruptedException interruptedException) {
                log.error("Interrupted exception: {}", interruptedException.getMessage());
            }
            if (Objects.isNull(message)) {
                continue;
            }

            log.info("messageType: {} | fromAbstractionId: {} | toAbstractionId: {} | hashCode: {}", message.getType(), message.getFromAbstractionId(), message.getToAbstractionId(), message.hashCode());

            ProtoPayload.Message innerMessage = message.getNetworkMessage().getMessage();

            if (ProtoPayload.Message.Type.PROC_INITIALIZE_SYSTEM.equals(innerMessage.getType())) {
                handleProcInitializeSystem(innerMessage);
            }
            else if (ProtoPayload.Message.Type.PROC_DESTROY_SYSTEM.equals(innerMessage.getType())) {
                messageReceiver.setShouldStop(true);
                break;
            }

            if (!abstractions.containsKey(message.getToAbstractionId())) {
                if (message.getToAbstractionId().contains(AbstractionType.NNAR.getKey())) {
                    registerAbstraction(new NNAtomicRegister(Util.getNamedAncestorAbstractionId(message.getToAbstractionId()), this));
                }
            }

            if (abstractions.containsKey(message.getToAbstractionId())) {
                if (!abstractions.get(message.getToAbstractionId()).handle(message) && shouldKeepMessage(message)) {
                    addMessageToQueue(message);
                }
            } else {
                addMessageToQueue(message);
            }
        }

        try {
            messageReceiverThread.join();
        } catch (InterruptedException e) {
            log.error("Interrupted exception while waiting termination of messageReceiverThread: {}", e.getMessage());
        }
        log.info("Process {} finished", processName);

    }

    private void handleProcInitializeSystem(ProtoPayload.Message message) {
        ProtoPayload.ProcInitializeSystem procInitializeSystem = message.getProcInitializeSystem();

        this.processes = procInitializeSystem.getProcessesList();

        this.process = getProcessByHostAndPort(this.process.getHost(), this.process.getPort()).get();

        this.systemId = message.getSystemId();
    }

    public void registerAbstraction(Abstraction abstraction) {
        abstractions.putIfAbsent(abstraction.getAbstractionId(), abstraction);
    }

    public void addMessageToQueue(ProtoPayload.Message message) {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            log.error("Error adding message to queue.");
        }
    }

    public Optional<ProtoPayload.ProcessId> getProcessByHostAndPort(String host, int port) {
        return processes.stream()
                .filter(p -> host.equals(p.getHost()) && p.getPort() == port)
                .findFirst();
    }

    private void registerToTheHub() {
        var procRegistration = ProtoPayload.ProcRegistration
                .newBuilder()
                .setOwner(process.getOwner())
                .setIndex(process.getIndex())
                .build();

        var procRegistrationMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.PROC_REGISTRATION)
                .setProcRegistration(procRegistration)
                .setMessageUuid(UUID.randomUUID().toString())
                .setToAbstractionId(AbstractionType.APP.getKey())
                .build();

        var networkMessage = ProtoPayload.NetworkMessage
                .newBuilder()
                .setSenderHost(process.getHost())
                .setSenderListeningPort(process.getPort())
                .setMessage(procRegistrationMessage)
                .build();

        var outerMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(networkMessage)
                .setToAbstractionId(procRegistrationMessage.getToAbstractionId())
                .setMessageUuid(UUID.randomUUID().toString())
                .build();

        MessageSender.send(outerMessage, hub.getHost(), hub.getPort());
    }

    private boolean shouldKeepMessage(ProtoPayload.Message message) {
        return ProtoPayload.Message.Type.PL_DELIVER.equals(message.getType()) && (
                ProtoPayload.Message.Type.NNAR_INTERNAL_VALUE.equals(message.getPlDeliver().getMessage().getType()) ||
                        ProtoPayload.Message.Type.NNAR_INTERNAL_ACK.equals(message.getPlDeliver().getMessage().getType())
        );
    }
}