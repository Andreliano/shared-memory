package org.example.consensus.algorithm;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.domain.Process;

@Getter
@AllArgsConstructor
public abstract class Abstraction {
    protected String abstractionId;
    protected Process process;

    public abstract boolean handle(ProtoPayload.Message message);
}
