package org.apache.hyracks.control.cc.scheduler;

public interface IWorkloadConfigInfo {
    public enum Type {
        UPSERT_GROUP,
        DELETE_GROUP,
        ENABLE_CONFIG,
        SET_WORKLOAD_PARAMETERS
    }

    public IWorkloadConfigInfo.Type getType();

}
