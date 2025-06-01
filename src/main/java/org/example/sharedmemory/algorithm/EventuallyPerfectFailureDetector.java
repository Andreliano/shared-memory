package org.example.sharedmemory.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.sharedmemory.communication.PerfectLink;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;
import org.example.sharedmemory.util.Util;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class EventuallyPerfectFailureDetector extends Abstraction {

    private final Set<ProtoPayload.ProcessId> alive;
    private final Set<ProtoPayload.ProcessId> suspected;
    private int delay;

    private static final int DELTA = 100;

    public EventuallyPerfectFailureDetector(String abstractionId, org.example.sharedmemory.domain.Process process) {
        super(abstractionId, process);
        alive = new CopyOnWriteArraySet<>(process.getProcesses());
        suspected = new CopyOnWriteArraySet<>();
        delay = DELTA;
        startTimer(delay);

        process.registerAbstraction(new PerfectLink(Util.getChildAbstractionId(abstractionId, AbstractionType.PL), process));
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case EPFD_TIMEOUT:
                handleEpfdTimeout();
                return true;
            case PL_DELIVER:
                ProtoPayload.PlDeliver plDeliver = message.getPlDeliver();
                return switch (plDeliver.getMessage().getType()) {
                    case EPFD_INTERNAL_HEARTBEAT_REQUEST -> {
                        handleHeartbeatRequest(plDeliver.getSender());
                        yield true;
                    }
                    case EPFD_INTERNAL_HEARTBEAT_REPLY -> {
                        handleHeartbeatReply(plDeliver.getSender());
                        yield true;
                    }
                    default -> false;
                };
            default:
                return false;
        }
    }

    private void handleEpfdTimeout() {
        log.info("[EPFD] Timeout triggered. Current suspected: {}, alive: {}", suspected, alive);

        Set<ProtoPayload.ProcessId> aliveSuspectIntersection = new CopyOnWriteArraySet<>(alive);
        aliveSuspectIntersection.retainAll(suspected);
        if (!aliveSuspectIntersection.isEmpty()) {
            delay += DELTA;
            log.info("[EPFD] Increasing delay to {} due to unstable suspects.", delay);
        }

        process.getProcesses().forEach(p -> {
            if (!alive.contains(p) && !suspected.contains(p)) {
                suspected.add(p);
                log.info("[EPFD] Suspecting process: " + p);
                triggerSuspect(p);
            } else if (alive.contains(p) && suspected.contains(p)) {
                suspected.remove(p);
                log.info("[EPFD] Restoring process: " + p);
                triggerRestore(p);
            }
            triggerPlSendHeartbeatRequest(p);
        });

        alive.clear();
        startTimer(delay);
    }

    private void handleHeartbeatRequest(ProtoPayload.ProcessId sender) {
        log.info("[EPFD] Received heartbeat request from: {}", sender);

        ProtoPayload.EpfdInternalHeartbeatReply epfdHeartbeatReply = ProtoPayload.EpfdInternalHeartbeatReply
                .newBuilder()
                .build();

        ProtoPayload.Message heartbeatReplyMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                .setEpfdInternalHeartbeatReply(epfdHeartbeatReply)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(sender)
                .setMessage(heartbeatReplyMessage)
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

    private void handleHeartbeatReply(ProtoPayload.ProcessId sender) {
        alive.add(sender);
        log.info("[EPFD] Received heartbeat reply from: {}", sender);
    }

    private void triggerSuspect(ProtoPayload.ProcessId p) {
        log.info("[EPFD] Triggering SUSPECT for: {}", p);

        ProtoPayload.EpfdSuspect epfdSuspect = ProtoPayload.EpfdSuspect
                .newBuilder()
                .setProcess(p)
                .build();

        ProtoPayload.Message suspectMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EPFD_SUSPECT)
                .setEpfdSuspect(epfdSuspect)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(suspectMessage);
    }

    private void triggerRestore(ProtoPayload.ProcessId p) {
        log.info("[EPFD] Triggering RESTORE for: {}", p);

        ProtoPayload.EpfdRestore epfdRestore = ProtoPayload.EpfdRestore
                .newBuilder()
                .setProcess(p)
                .build();

        ProtoPayload.Message restoreMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EPFD_RESTORE)
                .setEpfdRestore(epfdRestore)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                .setSystemId(process.getSystemId())
                .build();

        process.addMessageToQueue(restoreMessage);
    }

    private void triggerPlSendHeartbeatRequest(ProtoPayload.ProcessId p) {
        ProtoPayload.EpfdInternalHeartbeatRequest epfdHeartbeatRequest = ProtoPayload.EpfdInternalHeartbeatRequest
                .newBuilder()
                .build();

        ProtoPayload.Message heartbeatRequestMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                .setEpfdInternalHeartbeatRequest(epfdHeartbeatRequest)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        ProtoPayload.PlSend plSend = ProtoPayload.PlSend
                .newBuilder()
                .setDestination(p)
                .setMessage(heartbeatRequestMessage)
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

    private void startTimer(int delay) {
        log.info("[EPFD] Starting timer with delay: {}", delay);

        ProtoPayload.EpfdTimeout epfdTimeout = ProtoPayload.EpfdTimeout
                .newBuilder()
                .build();

        ProtoPayload.Message timeoutMessage = ProtoPayload.Message
                .newBuilder()
                .setType(ProtoPayload.Message.Type.EPFD_TIMEOUT)
                .setEpfdTimeout(epfdTimeout)
                .setFromAbstractionId(this.abstractionId)
                .setToAbstractionId(this.abstractionId)
                .setSystemId(process.getSystemId())
                .build();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.addMessageToQueue(timeoutMessage);
            }
        }, delay);
    }
}
