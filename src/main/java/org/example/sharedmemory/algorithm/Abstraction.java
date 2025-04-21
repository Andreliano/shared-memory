package org.example.sharedmemory.algorithm;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.Process;

@Getter
@AllArgsConstructor
public abstract class Abstraction {
    protected String abstractionId;
    protected Process process;

    public abstract boolean handle(ProtoPayload.Message message);
}
