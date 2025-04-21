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
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NNAtomicRegister extends Abstraction {
    private NNARValue nnarValue;
    private int acks;
    private ProtoPayload.Value writeVal;
    private int readId;
    private Map<String, NNARValue> readList;
    private ProtoPayload.Value readVal;
    private boolean isReading;

    public NNAtomicRegister(String abstractionId, Process process) {
        super(abstractionId, process);
        nnarValue = new NNARValue();
        acks = 0;
        writeVal = Util.buildUndefinedValue();
        readId = 0;
        readList = new ConcurrentHashMap<>();
        readVal = Util.buildUndefinedValue();
        isReading = false;

        process.registerAbstraction(new BestEffortBroadcast(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process));
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case NNAR_READ:
                log.info("READ!!!");
                handleNnarRead();
                return true;
            case NNAR_WRITE:
                log.info("WRITE!!!");
                handleNnarWrite(message.getNnarWrite());
                return true;
            case BEB_DELIVER:
                log.info("BROADCAST!!!");
                ProtoPayload.BebDeliver bebDeliver = message.getBebDeliver();
                switch (bebDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_READ:
                        ProtoPayload.NnarInternalRead nnarInternalRead = bebDeliver.getMessage().getNnarInternalRead();
                        handleBebDeliverInternalRead(bebDeliver.getSender(), nnarInternalRead.getReadId());
                        return true;
                    case NNAR_INTERNAL_WRITE:
                        ProtoPayload.NnarInternalWrite nnarInternalWrite = bebDeliver.getMessage().getNnarInternalWrite();
                        NNARValue value = new NNARValue(nnarInternalWrite.getTimestamp(), nnarInternalWrite.getWriterRank(), nnarInternalWrite.getValue());
                        handleBebDeliverInternalWrite(bebDeliver.getSender(), nnarInternalWrite.getReadId(), value);
                        return true;
                    default:
                        return false;
                }
            case PL_DELIVER:
                log.info("PL DELIVER!!!");
                ProtoPayload.PlDeliver plDeliver = message.getPlDeliver();
                switch (plDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_VALUE:
                        ProtoPayload.NnarInternalValue nnarInternalValue = plDeliver.getMessage().getNnarInternalValue();
                        if (nnarInternalValue.getReadId() == this.readId) {
                            NNARValue value = new NNARValue(nnarInternalValue.getTimestamp(), nnarInternalValue.getWriterRank(), nnarInternalValue.getValue());
                            triggerPlDeliverValue(plDeliver.getSender(), nnarInternalValue.getReadId(), value);
                            return true;
                        } else {
                            return nnarInternalValue.getReadId() < this.readId;
                        }
                    case NNAR_INTERNAL_ACK:
                        ProtoPayload.NnarInternalAck nnarInternalAck = plDeliver.getMessage().getNnarInternalAck();
                        if (nnarInternalAck.getReadId() == this.readId) {
                            triggerPlDeliverAck();
                            return true;
                        } else {
                            return nnarInternalAck.getReadId() < this.readId;
                        }
                    default:
                        return false;
                }
            default:
                log.info("DEFAULT!!!");
                return false;
        }
    }

    private void handleNnarRead() {
        readId++;

        acks = 0;

        readList = new ConcurrentHashMap<>();

        isReading = true;

        ProtoPayload.NnarInternalRead nnarInternalRead = ProtoPayload.NnarInternalRead
                .newBuilder()
                .setReadId(readId)
                .build();

        ProtoPayload.Message nnarInternalReadMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(nnarInternalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(nnarInternalReadMessage);
    }

    private void handleNnarWrite(ProtoPayload.NnarWrite nnarWrite) {
        readId++;

        writeVal = ProtoPayload.Value
                .newBuilder()
                .setV(nnarWrite.getValue().getV())
                .setDefined(true)
                .build();

        acks = 0;

        readList = new ConcurrentHashMap<>();

        ProtoPayload.NnarInternalRead nnarInternalRead = ProtoPayload.NnarInternalRead
                .newBuilder()
                .setReadId(readId)
                .build();

        ProtoPayload.Message nnarInternalReadMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_READ)
                .setNnarInternalRead(nnarInternalRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        triggerBebBroadcast(nnarInternalReadMessage);
    }

    private void handleBebDeliverInternalRead(ProtoPayload.ProcessId sender, int incomingReadId) {
        ProtoPayload.NnarInternalValue nnarInternalValue = ProtoPayload.NnarInternalValue
                .newBuilder()
                .setReadId(incomingReadId)
                .setTimestamp(this.nnarValue.getTimestamp())
                .setWriterRank(this.nnarValue.getWriterRank())
                .setValue(this.nnarValue.getValue())
                .build();

        ProtoPayload.Message nnarInternalValueMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_VALUE)
                .setNnarInternalValue(nnarInternalValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(nnarInternalValueMessage)
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

    private void handleBebDeliverInternalWrite(ProtoPayload.ProcessId sender, int incomingReadId, NNARValue incomingVal) {
        if (incomingVal.getTimestamp() > nnarValue.getTimestamp() || (incomingVal.getTimestamp() == nnarValue.getTimestamp() && incomingVal.getWriterRank() > nnarValue.getWriterRank())) {
            nnarValue = incomingVal;
        }

        ProtoPayload.NnarInternalAck nnarInternalAck = ProtoPayload.NnarInternalAck
                .newBuilder()
                .setReadId(incomingReadId)
                .build();

        ProtoPayload.Message nnarInternalAckMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_ACK)
                .setNnarInternalAck(nnarInternalAck)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(nnarInternalAckMessage)
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

    private void triggerPlDeliverValue(ProtoPayload.ProcessId sender, int incomingReadId, NNARValue incomingValue) {
        String senderId = sender.getOwner() + sender.getIndex();

        this.readList.put(senderId, incomingValue);

        if (this.readList.size() > (process.getProcesses().size() / 2)) {
            NNARValue highestValue = getHighestNnarValue();

            readVal = highestValue.getValue();

            readList.clear();

            ProtoPayload.NnarInternalWrite nnarInternalWrite;

            if (isReading) {
                nnarInternalWrite = ProtoPayload.NnarInternalWrite
                        .newBuilder()
                        .setReadId(incomingReadId)
                        .setTimestamp(highestValue.getTimestamp())
                        .setWriterRank(highestValue.getWriterRank())
                        .setValue(highestValue.getValue())
                        .build();
            } else {
                nnarInternalWrite = ProtoPayload.NnarInternalWrite
                        .newBuilder()
                        .setReadId(incomingReadId)
                        .setTimestamp(highestValue.getTimestamp() + 1)
                        .setWriterRank(process.getProcess().getRank())
                        .setValue(this.writeVal)
                        .build();
            }

            ProtoPayload.Message nnarInternalWriteMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.NNAR_INTERNAL_WRITE)
                    .setNnarInternalWrite(nnarInternalWrite)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(this.abstractionId)
                    .setSystemId(process.getSystemId())
                    .build();

            triggerBebBroadcast(nnarInternalWriteMessage);
        }
    }

    private void triggerPlDeliverAck() {
        acks++;
        if (acks > (process.getProcesses().size() / 2)) {
            acks = 0;
            if (isReading) {
                isReading = false;
                ProtoPayload.NnarReadReturn nnarReadReturn = ProtoPayload.NnarReadReturn
                        .newBuilder()
                        .setValue(readVal)
                        .build();

                ProtoPayload.Message nnarReadReturnMessage = ProtoPayload.Message
                        .newBuilder()
                        .setType(ProtoPayload.Message.Type.NNAR_READ_RETURN)
                        .setNnarReadReturn(nnarReadReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                process.addMessageToQueue(nnarReadReturnMessage);
            } else {
                ProtoPayload.NnarWriteReturn nnarWriteReturn = ProtoPayload.NnarWriteReturn
                        .newBuilder()
                        .build();

                ProtoPayload.Message nnarWriteReturnMessage = ProtoPayload.Message
                        .newBuilder()
                        .setType(ProtoPayload.Message.Type.NNAR_WRITE_RETURN)
                        .setNnarWriteReturn(nnarWriteReturn)
                        .setFromAbstractionId(this.abstractionId)
                        .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                        .setSystemId(process.getSystemId())
                        .build();

                process.addMessageToQueue(nnarWriteReturnMessage);
            }
        }
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

    private NNARValue getHighestNnarValue() {
        return this.readList.values().stream()
                .min(Comparator
                        .comparingInt(NNARValue::getTimestamp)
                        .thenComparingInt(NNARValue::getWriterRank))
                .orElseThrow(() -> new NoSuchElementException("No NNARValue found"));
    }
}
