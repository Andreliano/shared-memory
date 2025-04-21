package org.example.sharedmemory.util;

import org.example.sharedmemory.communication.ProtoPayload;
import org.example.sharedmemory.domain.AbstractionType;

public class Util {
    public static final String HUB_ID = "hub";

    public static String getParentAbstractionId(String childAbstractionId) {
        return childAbstractionId.substring(0, childAbstractionId.lastIndexOf("."));
    }

    public static String getChildAbstractionId(String parentAbstractionId, AbstractionType childAbstractionType) {
        return parentAbstractionId + "." + childAbstractionType.getKey();
    }

    public static String getNamedAncestorAbstractionId(String abstractionId) {
        return abstractionId.substring(0, abstractionId.indexOf("]") + 1);
    }

    public static String getNamedAbstractionId(String parentAbstractionId, AbstractionType abstractionType, String name) {
        return getChildAbstractionId(parentAbstractionId, abstractionType) + "[" + name + "]";
    }

    public static String getInternalNameFromAbstractionId(String abstractionId) {
        return abstractionId.substring(abstractionId.indexOf("[") + 1, abstractionId.indexOf("]"));
    }

    public static ProtoPayload.Value buildUndefinedValue() {
        return ProtoPayload.Value
                .newBuilder()
                .setDefined(false)
                .build();
    }
}
