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
        process.registerAbstraction(new BestEffortBroadcast(Util.getChildAbstractionId(abstractionId, AbstractionType.BEB), process)); // register app.beb abstraction
        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process)); // register app.pl abstraction
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
                ProtoPayload.PlDeliver plDeliver = message.getPlDeliver();
                return switch (plDeliver.getMessage().getType()) {
                    case APP_BROADCAST -> {
                        handleAppBroadcast(plDeliver.getMessage().getAppBroadcast());
                        yield true;
                    }
                    case APP_READ -> {
                        handleAppRead(plDeliver.getMessage().getAppRead());
                        yield true;
                    }
                    case APP_WRITE -> {
                        handleAppWrite(plDeliver.getMessage().getAppWrite());
                        yield true;
                    }
                    default -> false;
                };
            case BEB_DELIVER:
                ProtoPayload.Message innerMessage = message.getBebDeliver().getMessage();
                triggerPlSend(innerMessage);
                return true;
            case NNAR_READ_RETURN:
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId());
                return true;
            case NNAR_WRITE_RETURN:
                handleNnarWriteReturn(message.getFromAbstractionId());
                return true;
        }
        return false;
    }

    private void handleAppBroadcast(ProtoPayload.AppBroadcast appBroadcast) {
        ProtoPayload.AppValue appValue = ProtoPayload.AppValue
                .newBuilder()
                .setValue(appBroadcast.getValue())
                .build();

        ProtoPayload.Message appValueMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_VALUE)
                .setAppValue(appValue)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.BebBroadcast bebBroadcast = ProtoPayload.BebBroadcast
                .newBuilder()
                .setMessage(appValueMessage)
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

    private void handleAppRead(ProtoPayload.AppRead appRead) {
        String nnarAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appRead.getRegister());
        process.registerAbstraction(new NNAtomicRegister(nnarAbstractionId, process));

        ProtoPayload.NnarRead nnarRead = ProtoPayload.NnarRead
                .newBuilder()
                .build();

        ProtoPayload.Message nnarReadMessage = ProtoPayload.Message
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

        ProtoPayload.NnarWrite nnarWrite = ProtoPayload.NnarWrite
                .newBuilder()
                .setValue(appWrite.getValue())
                .build();

        ProtoPayload.Message nnarWriteMessage = ProtoPayload.Message
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
        ProtoPayload.AppReadReturn appReadReturn = ProtoPayload.AppReadReturn
                .newBuilder()
                .setRegister(Util.getInternalNameFromAbstractionId(fromAbstractionId))
                .setValue(nnarReadReturn.getValue())
                .build();

        ProtoPayload.Message appReadReturnMessage = ProtoPayload.Message
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
        ProtoPayload.AppWriteReturn appWriteReturn = ProtoPayload.AppWriteReturn
                .newBuilder()
                .setRegister(Util.getInternalNameFromAbstractionId(fromAbstractionId))
                .build();

        ProtoPayload.Message appWriteReturnMessage = ProtoPayload.Message
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
        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(process.getHub())
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
}
