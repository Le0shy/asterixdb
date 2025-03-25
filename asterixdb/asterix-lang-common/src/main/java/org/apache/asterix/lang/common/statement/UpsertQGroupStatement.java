package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.SchedulerConfigUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.*;

import java.util.HashMap;
import java.util.Map;

public class UpsertQGroupStatement extends AbstractStatement {
    private final String configName;
    private final AdmObjectNode upsertNode;
    private final Namespace namespace;

    public UpsertQGroupStatement(Namespace namespace, String configName, RecordConstructor expr)
            throws CompilationException {
        this.namespace = namespace;
        this.configName = configName;
        this.upsertNode = SchedulerConfigUtil.validateUpsertQgroupNode(expr);
    }

    public String getConfigName() {
        return configName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public Map<String, Long> getUpsertQueryGroups() {
        AdmArrayNode arrayNode = (AdmArrayNode) upsertNode.get("list");
        Map<String, Long> upsertQueryGroups = new HashMap<>();

        for (IAdmNode iAdmNode : arrayNode) {
            AdmObjectNode abn = (AdmObjectNode) iAdmNode;
            AdmBigIntNode abin = (AdmBigIntNode) abn.get("priority");
            long priority = abin.get();

            AdmStringNode asn = (AdmStringNode) abn.get("name");
            String name = asn.get();

            upsertQueryGroups.put(name, priority);
        }

        return upsertQueryGroups;
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
