package org.example.consensus.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.example.consensus.communication.ProtoPayload;
import org.example.consensus.util.Util;

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
