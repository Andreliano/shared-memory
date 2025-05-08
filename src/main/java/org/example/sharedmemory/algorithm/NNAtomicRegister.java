package org.example.sharedmemory.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.communication.PerfectLink;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.domain.NNARValue;
import org.example.sharedmemory.domain.Process;
import org.example.sharedmemory.util.Util;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
public class NNAtomicRegister extends Abstraction {
    private NNARValue currentValue;
    private int ackCount;
    private ProtoPayload.Value writeValue;
    private int currentReadId;
    private final Map<String, NNARValue> readResponses;
    private ProtoPayload.Value readResult;
    private boolean isReadInProgress;

    public NNAtomicRegister(String abstractionId, Process process) {
        super(abstractionId, process);
        currentValue = new NNARValue();
        ackCount = 0;
        writeValue = Util.buildUndefinedValue();
        currentReadId = 0;
        readResponses = new ConcurrentHashMap<>();
        readResult = Util.buildUndefinedValue();
        isReadInProgress = false;

        registerCoreAbstractions(process);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        return switch (message.getType()) {
            case NNAR_READ -> {
                handleReadRequest();
                yield true;
            }
            case NNAR_WRITE -> {
                handleWriteRequest(message.getNnarWrite());
                yield true;
            }
            case BEB_DELIVER -> {
                handleBebDeliver(message.getBebDeliver());
                yield true;
            }
            case PL_DELIVER -> {
                handlePlDeliver(message.getPlDeliver());
                yield true;
            }
            default -> false;
        };
    }

    private void handleReadRequest() {
        log.info("Handling NNAR_READ request");
        initializeRead(true, Util.buildUndefinedValue());
    }

    private void handleWriteRequest(ProtoPayload.NnarWrite write) {
        log.info("Handling NNAR_WRITE request with value: {}", write.getValue().getV());
        ProtoPayload.Value value = ProtoPayload.Value.newBuilder()
                .setV(write.getValue().getV())
                .setDefined(true)
                .build();
        initializeRead(false, value);
    }

    private void initializeRead(boolean isRead, ProtoPayload.Value valueToWrite) {
        currentReadId++;
        ackCount = 0;
        readResponses.clear();
        isReadInProgress = isRead;
        writeValue = valueToWrite;

        var internalRead = ProtoPayload.NnarInternalRead.newBuilder()
                .setReadId(currentReadId)
                .build();

        var message = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(internalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        broadcast(message);
    }

    private void handleBebDeliver(ProtoPayload.BebDeliver deliver) {
        var msg = deliver.getMessage();
        switch (msg.getType()) {
            case NNAR_INTERNAL_READ -> {
                handleInternalRead(deliver.getSender(), msg.getNnarInternalRead().getReadId());
            }
            case NNAR_INTERNAL_WRITE -> {
                var write = msg.getNnarInternalWrite();
                var val = new NNARValue(write.getTimestamp(), write.getWriterRank(), write.getValue());
                handleInternalWrite(deliver.getSender(), write.getReadId(), val);
            }
            default -> {
            }
        }
    }

    private void handleInternalRead(ProtoPayload.ProcessId sender, int currentReadId) {
        var response = ProtoPayload.NnarInternalValue.newBuilder()
                .setReadId(currentReadId)
                .setTimestamp(currentValue.getTimestamp())
                .setWriterRank(currentValue.getWriterRank())
                .setValue(currentValue.getValue())
                .build();

        sendPlMessage(sender, ProtoPayload.Message.Type.NNAR_INTERNAL_VALUE,
                builder -> builder.setNnarInternalValue(response));
    }

    private void handleInternalWrite(ProtoPayload.ProcessId sender, int readId, NNARValue newValue) {
        if (shouldUpdateValue(newValue)) {
            log.info("Updating current value to timestamp = {}, writerRank = {}, value = {}",
                    newValue.getTimestamp(), newValue.getWriterRank(), newValue.getValue().getV());
            currentValue = newValue;
        }

        var ack = ProtoPayload.NnarInternalAck.newBuilder()
                .setReadId(readId)
                .build();

        sendPlMessage(sender, ProtoPayload.Message.Type.NNAR_INTERNAL_ACK,
                builder -> builder.setNnarInternalAck(ack));
    }

    private void handlePlDeliver(ProtoPayload.PlDeliver deliver) {
        var msg = deliver.getMessage();
        switch (msg.getType()) {
            case NNAR_INTERNAL_VALUE -> {
                if (msg.getNnarInternalValue().getReadId() == currentReadId) {
                    var value = msg.getNnarInternalValue();
                    NNARValue val = new NNARValue(value.getTimestamp(), value.getWriterRank(), value.getValue());
                    processValue(deliver.getSender(), value.getReadId(), val);
                }
            }
            case NNAR_INTERNAL_ACK -> {
                var ack = msg.getNnarInternalAck();
                if (ack.getReadId() == currentReadId) {
                    processAck();
                }
            }
            default -> {
            }
        }
    }

    private void processValue(ProtoPayload.ProcessId sender, int readId, NNARValue val) {
        readResponses.put(sender.getOwner() + sender.getIndex(), val);

        if (readResponses.size() > process.getProcesses().size() / 2) {
            NNARValue highest = readResponses.values().stream()
                    .max(Comparator.comparingInt(NNARValue::getTimestamp)
                            .thenComparingInt(NNARValue::getWriterRank))
                    .orElseThrow();

            readResult = highest.getValue();
            readResponses.clear();

            var writeMsg = ProtoPayload.NnarInternalWrite.newBuilder()
                    .setReadId(readId)
                    .setTimestamp(isReadInProgress ? highest.getTimestamp() : highest.getTimestamp() + 1)
                    .setWriterRank(isReadInProgress ? highest.getWriterRank() : process.getProcess().getRank())
                    .setValue(isReadInProgress ? highest.getValue() : writeValue)
                    .build();

            log.info("Broadcasting internal write for readId {} with value = {}", readId,
                    writeMsg.getValue().getV());


            ProtoPayload.Message nnarInternalWriteMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_WRITE)
                    .setNnarInternalWrite(writeMsg)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            broadcast(nnarInternalWriteMessage);
        }
    }

    private void processAck() {
        ackCount++;
        if (ackCount > process.getProcesses().size() / 2) {
            ackCount = 0;
            ProtoPayload.Message msg;

            if (isReadInProgress) {
                isReadInProgress = false;

                msg = ProtoPayload.Message
                        .newBuilder()
                        .setType(ProtoPayload.Message.Type.NNAR_READ_RETURN)
                        .setNnarReadReturn(ProtoPayload.NnarReadReturn.newBuilder().setValue(readResult).build())
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                log.info("Completed NNAR_READ with value = {}", readResult.getV());
            } else {

                msg = ProtoPayload.Message
                        .newBuilder()
                        .setType(ProtoPayload.Message.Type.NNAR_READ_RETURN)
                        .setNnarReadReturn(ProtoPayload.NnarReadReturn.newBuilder().setValue(readResult).build())
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                log.info("Completed NNAR_WRITE");
            }

            process.addMessageToQueue(msg);
        }
    }

    private void broadcast(ProtoPayload.Message msg) {
        ProtoPayload.BebBroadcast broadcast = ProtoPayload.BebBroadcast.newBuilder()
                .setMessage(msg)
                .build();

        ProtoPayload.Message bebBroadcastMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(broadcast)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.BEB))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }

    private void sendPlMessage(ProtoPayload.ProcessId dest, ProtoPayload.Message.Type type,
                               Consumer<ProtoPayload.Message.Builder> setPayload) {
        var msgBuilder = buildMessage(type);
        setPayload.accept(msgBuilder);
        var message = msgBuilder.build();

        var plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(dest)
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

    private ProtoPayload.Message.Builder buildMessage(ProtoPayload.Message.Type type) {
        return ProtoPayload.Message.newBuilder()
                .setType(type)
                .setFromAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId());
    }

    private boolean shouldUpdateValue(NNARValue val) {
        return val.getTimestamp() > currentValue.getTimestamp() ||
                (val.getTimestamp() == currentValue.getTimestamp() &&
                        val.getWriterRank() > currentValue.getWriterRank());
    }

    private void registerCoreAbstractions(Process process) {
        log.info("Registering abstractions for process: {}", process.getSystemId());
        process.registerAbstraction(new BEB(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }
}