package org.example.consensus.domain;

import lombok.Getter;
import lombok.Setter;
import org.example.consensus.communication.ProtoPayload;

@Setter
@Getter
public class EpochState {

    private int valTimestamp;
    private ProtoPayload.Value val;

    public EpochState(int valTimestamp, ProtoPayload.Value val) {
        this.valTimestamp = valTimestamp;
        this.val = val;
    }

}
