package org.example.sharedmemory.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.communication.PerfectLink;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.domain.Process;
import org.example.sharedmemory.util.Util;

@Slf4j
public class Application extends Abstraction {
    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        registerCoreAbstractions(process);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        return switch (message.getType()) {
            case PL_DELIVER -> handlePlDeliver(message.getPlDeliver());
            case BEB_DELIVER -> {
                triggerPlSend(message.getBebDeliver().getMessage());
                yield true;
            }
            case NNAR_READ_RETURN -> {
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId());
                yield true;
            }
            case NNAR_WRITE_RETURN -> {
                handleNnarWriteReturn(message.getFromAbstractionId());
                yield true;
            }
            default -> {
                log.warn("Unhandled message type: {}", message.getType());
                yield false;
            }
        };
    }

    private boolean handlePlDeliver(ProtoPayload.PlDeliver deliver) {
        ProtoPayload.Message innerMessage = deliver.getMessage();
        return switch (innerMessage.getType()) {
            case APP_BROADCAST -> {
                handleAppBroadcast(innerMessage.getAppBroadcast());
                yield true;
            }
            case APP_READ -> {
                handleAppRead(innerMessage.getAppRead());
                yield true;
            }
            case APP_WRITE -> {
                handleAppWrite(innerMessage.getAppWrite());
                yield true;
            }
            default -> {
                log.warn("Unhandled PL_DELIVER message type: {}", innerMessage.getType());
                yield false;
            }
        };
    }


    private void handleAppBroadcast(ProtoPayload.AppBroadcast appBroadcast) {
        var appValue = ProtoPayload.AppValue
                .newBuilder()
                .setValue(appBroadcast.getValue())
                .build();

        var appValueMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_VALUE)
                .setAppValue(appValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        var bebBroadcast = ProtoPayload.BebBroadcast
                .newBuilder()
                .setMessage(appValueMessage)
                .build();

        var bebBroadcastMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.BEB))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(bebBroadcastMessage);
    }

    private void handleAppRead(ProtoPayload.AppRead appRead) {
        String nnarAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appRead.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        var nnarRead = ProtoPayload.NnarRead
                .newBuilder()
                .build();

        var nnarReadMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_READ)
                .setNnarRead(nnarRead)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarReadMessage);
    }

    private void handleAppWrite(ProtoPayload.AppWrite appWrite) {
        // register app.nnar[register] abstraction if not present
        String nnarAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appWrite.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        var nnarWrite = ProtoPayload.NnarWrite
                .newBuilder()
                .setValue(appWrite.getValue())
                .build();

        var nnarWriteMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.NNAR_WRITE)
                .setNnarWrite(nnarWrite)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(nnarAbstractionId)
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(nnarWriteMessage);
    }


    private void handleNnarReadReturn(ProtoPayload.NnarReadReturn nnarReadReturn, String fromAbstractionId) {
        var appReadReturn = ProtoPayload.AppReadReturn
                .newBuilder()
                .setRegister(Util.getInternalNameFromAbstractionId(fromAbstractionId))
                .setValue(nnarReadReturn.getValue())
                .build();

        var appReadReturnMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_READ_RETURN)
                .setAppReadReturn(appReadReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appReadReturnMessage);
    }

    private void handleNnarWriteReturn(String fromAbstractionId) {
        var appWriteReturn = ProtoPayload.AppWriteReturn
                .newBuilder()
                .setRegister(Util.getInternalNameFromAbstractionId(fromAbstractionId))
                .build();

        var appWriteReturnMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_WRITE_RETURN)
                .setAppWriteReturn(appWriteReturn)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appWriteReturnMessage);
    }

    private void triggerPlSend(ProtoPayload.Message message) {
        var plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(process.getHub())
                .setMessage(message)
                .build();

        var plSendMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(Util.getChildAbstractionId(this.abstractionId, AbstractionType.PL))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(plSendMessage);
    }

    private void registerCoreAbstractions(Process process) {
        process.registerAbstraction(new BestEffortBroadcast(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process)); // register app.beb abstraction
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process)); // register app.pl abstraction
    }
}