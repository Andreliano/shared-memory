package org.example.consensus.algorithm;

import lombok.extern.slf4j.Slf4j;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.domain.AbstractionType;
import org.example.consensus.util.Util;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class EventualLeaderDetector extends Abstraction {

    private final Set<ProtoPayload.ProcessId> suspected;
    private ProtoPayload.ProcessId leader;

    public EventualLeaderDetector(String abstractionId, org.example.consensus.domain.Process process) {
        super(abstractionId, process);
        suspected = new CopyOnWriteArraySet<>();

        process.registerAbstraction(new EventuallyPerfectFailureDetector(Util.getChildAbstractionId(abstractionId, AbstractionType.EPFD), process));
    }

    @Override
    public boolean handle(ProtoPayload.Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                ProtoPayload.ProcessId suspectedProcess = message.getEpfdSuspect().getProcess();
                suspected.add(suspectedProcess);
                log.info("[ELD] Process suspected: {}", suspectedProcess);
                performCheck();
                return true;
            case EPFD_RESTORE:
                ProtoPayload.ProcessId restoredProcess = message.getEpfdSuspect().getProcess();
                suspected.remove(restoredProcess);
                log.info("[ELD] Process restored: {}", restoredProcess);
                performCheck();
                return true;
            default:
                return false;
        }
    }

    private void performCheck() {
        Set<ProtoPayload.ProcessId> notSuspected = new CopyOnWriteArraySet<>(process.getProcesses());
        notSuspected.removeAll(suspected);
        ProtoPayload.ProcessId maxRankedProcess = Util.getMaxRankedProcess(notSuspected);

        log.info("[ELD] Performing leader check. Not suspected: {}", notSuspected);

        if (maxRankedProcess != null && !maxRankedProcess.equals(leader)) {
            leader = maxRankedProcess;
            log.info("[ELD] New leader elected: {}", leader);

            ProtoPayload.EldTrust eldTrust = ProtoPayload.EldTrust
                    .newBuilder()
                    .setProcess(leader)
                    .build();

            ProtoPayload.Message trustMessage = ProtoPayload.Message
                    .newBuilder()
                    .setType(ProtoPayload.Message.Type.ELD_TRUST)
                    .setEldTrust(eldTrust)
                    .setFromAbstractionId(this.abstractionId)
                    .setToAbstractionId(Util.getParentAbstractionId(this.abstractionId))
                    .setSystemId(process.getSystemId())
                    .build();

            process.addMessageToQueue(trustMessage);
        }
    }
}
