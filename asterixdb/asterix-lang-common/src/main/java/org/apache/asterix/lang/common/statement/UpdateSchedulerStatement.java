package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.SchedulerConfigUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.*;

public class UpdateSchedulerStatement extends AbstractStatement {
    private final String configName;
    private final AdmObjectNode updateSchedulerNode;
    public static final String FIELD_NAME_DEFAULT_PRIORITY = "defaultPriority";
    public static final String FIELD_NAME_SHORT_MEMORY_QUOTA = "shortMemoryPercent";
    public static final String FIELD_NAME_SHORT_CPU_QUOTA = "shortCPUQuota";

    public UpdateSchedulerStatement(String configName, RecordConstructor expr) throws CompilationException {
        this.configName = configName;
        this.updateSchedulerNode = SchedulerConfigUtil.validateUpdateSchedulerNode(expr);
    }

    public String getConfigName() {
        return configName;
    }

    public long getDefaultPriority() {
        AdmBigIntNode admNode = (AdmBigIntNode) updateSchedulerNode.get(FIELD_NAME_DEFAULT_PRIORITY);
        return admNode.get();
    }

    public double getShortMemoryPercent() {
        AdmDoubleNode admNode = (AdmDoubleNode) updateSchedulerNode.get(FIELD_NAME_SHORT_MEMORY_QUOTA);
        return admNode.get();
    }

    public long getShortCPUQuota() {
        AdmBigIntNode admNode = (AdmBigIntNode) updateSchedulerNode.get(FIELD_NAME_SHORT_CPU_QUOTA);
        return admNode.get();
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE_SCHEDULER;
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
