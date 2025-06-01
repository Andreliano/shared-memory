package org.example.consensus.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.consensus.communication.PerfectLink;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.domain.AbstractionType;
import org.example.consensus.domain.Process;
import org.example.consensus.util.Util;

@Slf4j
public class Application extends Abstraction {
    public Application(String abstractionId, Process process) {
        super(abstractionId, process);
        log.info("Initializing Application abstraction with ID: {}", abstractionId);
        registerCoreAbstractions(process);
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        log.info("Application.handle() received message of type: {}", message.getType());
        return switch (message.getType()) {
            case PL_DELIVER -> {
                handlePlDeliver(message.getPlDeliver());
                yield true;
            }
            case BEB_DELIVER -> {
                log.info("Handling BEB_DELIVER message, rebroadcasting with PL_SEND");
                triggerPlSend(message.getBebDeliver().getMessage());
                yield true;
            }
            case NNAR_READ_RETURN -> {
                log.info("Received NNAR_READ_RETURN from: {}", message.getFromAbstractionId());
                handleNnarReadReturn(message.getNnarReadReturn(), message.getFromAbstractionId());
                yield true;
            }
            case NNAR_WRITE_RETURN -> {
                log.info("Received NNAR_WRITE_RETURN from: {}", message.getFromAbstractionId());
                handleNnarWriteReturn(message.getFromAbstractionId());
                yield true;
            }
            case UC_DECIDE -> {
                log.info("Received UC_DECIDE from: {}", message.getFromAbstractionId());
                handleUcDecide(message.getUcDecide());
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
        log.info("PL_DELIVER received, inner message type: {}", innerMessage.getType());
        return switch (innerMessage.getType()) {
            case APP_BROADCAST -> {
                log.info("Handling APP_BROADCAST with value: {}", innerMessage.getAppBroadcast().getValue());
                handleAppBroadcast(innerMessage.getAppBroadcast());
                yield true;
            }
            case APP_READ -> {
                log.info("Handling APP_READ for register: {}", innerMessage.getAppRead().getRegister());
                handleAppRead(innerMessage.getAppRead());
                yield true;
            }
            case APP_WRITE -> {
                log.info("Handling APP_WRITE for register: {}, value: {}", innerMessage.getAppWrite().getRegister(), innerMessage.getAppWrite().getValue());
                handleAppWrite(innerMessage.getAppWrite());
                yield true;
            }
            case APP_PROPOSE -> {
                log.info("Handling APP_PROPOSE with value: {}", innerMessage.getAppPropose().getValue());
                handleAppPropose(innerMessage.getAppPropose());
                yield true;
            }
            default -> {
                log.warn("Unhandled PL_DELIVER message type: {}", innerMessage.getType());
                yield false;
            }
        };
    }


    private void handleAppBroadcast(ProtoPayload.AppBroadcast appBroadcast) {
        log.info("Broadcasting value: {} using BEB", appBroadcast.getValue());

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

        log.info("Sending BEB_BROADCAST message to abstraction: {}", bebBroadcastMessage.getToAbstractionId());
        process.addMessageToQueue(bebBroadcastMessage);
    }

    private void handleAppRead(ProtoPayload.AppRead appRead) {
        String nnarAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appRead.getRegister());
        log.info("Handling APP_READ for register: {}, abstractionId: {}", appRead.getRegister(), nnarAbstractionId);

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

        log.info("Sending NNAR_READ message to: {}", nnarAbstractionId);
        process.addMessageToQueue(nnarReadMessage);
    }

    private void handleAppWrite(ProtoPayload.AppWrite appWrite) {
        String nnarAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.NNAR, appWrite.getRegister());
        log.info("Handling APP_WRITE for register: {}, value: {}, abstractionId: {}", appWrite.getRegister(), appWrite.getValue(), nnarAbstractionId);

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

        log.info("Sending NNAR_WRITE message to: {}", nnarAbstractionId);
        process.addMessageToQueue(nnarWriteMessage);
    }

    private void handleAppPropose(ProtoPayload.AppPropose appPropose) {
        // register app.uc[topic] abstraction

        String uniformConsensusAbstractionId = Util.getNamedAbstractionId(this.abstractionId, AbstractionType.UC, appPropose.getTopic());
        log.info("Handling APP_PROPOSE for value: {}, topic: {}, abstractionId: {}", appPropose.getValue(), appPropose.getTopic(), uniformConsensusAbstractionId);

        process.registerAbstraction(new UniformConsensus(uniformConsensusAbstractionId, process));

        ProtoPayload.UcPropose ucPropose = ProtoPayload.UcPropose
                .newBuilder()
                .setValue(appPropose.getValue())
                .build();

        ProtoPayload.Message ucProposeMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.UC_PROPOSE)
                .setUcPropose(ucPropose)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getNamedAbstractionId(this.abstractionId, AbstractionType.UC, appPropose.getTopic()))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(ucProposeMessage);
    }

    private void handleNnarReadReturn(ProtoPayload.NnarReadReturn nnarReadReturn, String fromAbstractionId) {
        String register = Util.getInternalNameFromAbstractionId(fromAbstractionId);
        log.info("Received NNAR_READ_RETURN from abstraction: {}, register: {}, value: {}", fromAbstractionId, register, nnarReadReturn.getValue());

        var appReadReturn = ProtoPayload.AppReadReturn
                .newBuilder()
                .setRegister(register)
                .setValue(nnarReadReturn.getValue())
                .build();

        var appReadReturnMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_READ_RETURN)
                .setAppReadReturn(appReadReturn)
                .setFromAbstractionId(register)
                .setToAbstractionId(Util.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appReadReturnMessage);
    }

    private void handleNnarWriteReturn(String fromAbstractionId) {
        String register = Util.getInternalNameFromAbstractionId(fromAbstractionId);
        log.info("Received NNAR_WRITE_RETURN from abstraction: {}, register: {}", fromAbstractionId, register);

        var appWriteReturn = ProtoPayload.AppWriteReturn
                .newBuilder()
                .setRegister(register)
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

    private void handleUcDecide(ProtoPayload.UcDecide ucDecide) {
        ProtoPayload.AppDecide appDecide = ProtoPayload.AppDecide
                .newBuilder()
                .setValue(ucDecide.getValue())
                .build();

        ProtoPayload.Message appDecideMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.APP_DECIDE)
                .setAppDecide(appDecide)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.HUB_ID)
                .setSystemId(process.getSystemId())
                .build();

        triggerPlSend(appDecideMessage);
    }

    private void triggerPlSend(ProtoPayload.Message message) {
        log.info("Triggering PL_SEND to hub for message type: {}", message.getType());

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
        String bebId = Util.getChildAbstractionId(abstractionId, AbstractionType.BEB);
        String plId = Util.getChildAbstractionId(abstractionId, AbstractionType.PL);
        log.info("Registering core abstractions: BEB -> {}, PL -> {}", bebId, plId);

        process.registerAbstraction(new BEB(bebId, process));
        process.registerAbstraction(new PerfectLink(plId, process));
    }
}