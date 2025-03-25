package org.apache.asterix.lang.common.statement;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.SchedulerConfigUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.*;

import java.util.*;

public class DeleteQGroupStatement extends AbstractStatement {
    private final String configName;
    private final AdmObjectNode deleteNode;
    private final Namespace namespace;

    public DeleteQGroupStatement(Namespace namespace, String configName, RecordConstructor expr)
            throws CompilationException {
        this.namespace = namespace;
        this.configName = configName;
        this.deleteNode = SchedulerConfigUtil.validateDeleteQgroupNode(expr);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }
    public String getConfigName() {
        return configName;
    };

    public List<String> getDeleteQueryGroups() {
        List<String> deleteQGroups = new ArrayList<>();
        AdmArrayNode arrayNode = (AdmArrayNode) deleteNode.get("list");

        for (IAdmNode iAdmNode : arrayNode) {
            deleteQGroups.add(((AdmStringNode) iAdmNode).get());
        }
        return deleteQGroups;
    }

    @Override
    public Kind getKind() {
        return Kind.DELETE_QGROUP;
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