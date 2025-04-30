package org.apache.hyracks.control.cc.scheduler;

import java.util.Map;

public class UpsertGroupInfo implements IWorkloadConfigInfo {
    private final Map<String, Long> groupToUpsert;
    private final Type type = Type.UPSERT_GROUP;

    public UpsertGroupInfo(Map<String, Long> groupToDelete) {
        this.groupToUpsert = groupToDelete;
    }

    @Override
    public Type getType() {
        return type;
    }

    public Map<String, Long> getGroupToUpsert() {
        return groupToUpsert;
    }
}
