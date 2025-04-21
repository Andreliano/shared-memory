package org.example.sharedmemory.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.util.Util;

@Getter
@Setter
@AllArgsConstructor
public class NNARValue {

    private int timestamp;
    private int writerRank;
    private ProtoPayload.Value value;

    public NNARValue() {
        timestamp = 0;
        writerRank = 0;
        value = Util.buildUndefinedValue();
    }
}
