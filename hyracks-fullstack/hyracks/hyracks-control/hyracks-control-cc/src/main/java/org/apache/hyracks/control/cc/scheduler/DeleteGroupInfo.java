package org.apache.hyracks.control.cc.scheduler;

import java.util.List;

public class DeleteGroupInfo implements IWorkloadConfigInfo {
    private final List<String> groupToDelete;

    private final Type type = Type.DELETE_GROUP;

    public DeleteGroupInfo(List<String> groupToDelete) {
        this.groupToDelete = groupToDelete;
    }

    public List<String> getGroupToDelete() {
        return groupToDelete;
    }

    @Override
    public Type getType() {
        return type;
    }
}
