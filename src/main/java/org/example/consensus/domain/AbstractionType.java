package org.example.consensus.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum AbstractionType {
    APP("app"),
    PL("pl"),
    BEB("beb"),
    NNAR("nnar"),
    EPFD("epfd"),
    ELD("eld"),
    EP("ep"),
    EC("ec"),
    UC("uc");

    private final String key;
}
