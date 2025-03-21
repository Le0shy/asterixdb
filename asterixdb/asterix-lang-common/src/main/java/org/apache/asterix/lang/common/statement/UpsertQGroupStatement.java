package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.util.SchedulerConfigUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.*;
import org.apache.iceberg.data.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpsertQGroupStatement extends AbstractStatement {
    private final String configName;
    private final AdmObjectNode upsertNode;

    public UpsertQGroupStatement(String configName, Record expr) {
        this.configName = configName;
        this.upsertNode = SchedulerConfigUtil.validateUpsertGroupNode(expr);
    }

    public String getConfigName() {
        return configName;
    };

    public Map<Long, List<String>> getUpsertQueryGroups() {
        AdmArrayNode arrayNode = (AdmArrayNode) upsertNode.get(FIELD_NAME_QUERY_GROUP);
        Map<Long, List<String>> priorityToGroup = new HashMap<>();

        for (IAdmNode iAdmNode : arrayNode) {
            AdmObjectNode abn = (AdmObjectNode)iAdmNode;
            AdmBigIntNode abin = (AdmBigIntNode) abn.get(FIELD_NAME_QG_PRIORITY);
            long priority = abin.get();

            List<String> groupNames = new ArrayList<>();
            AdmArrayNode groupListNode = (AdmArrayNode) abn.get(FIELD_NAME_QG_GROUPLIST);
            for(IAdmNode groupName: groupListNode) {
                groupNames.add(((AdmStringNode) groupName).get());
            }

            priorityToGroup.put(priority, groupNames);
        }

        return priorityToGroup;
    }

    @Override
    public Kind getKind() {
        return Kind.UPSERT_QGROUP;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }
}
