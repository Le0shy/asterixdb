package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class SchedulerConfigDropStatement extends AbstractStatement {
    private final String configName;
    private final boolean ifExists;

    public SchedulerConfigDropStatement(String configName, boolean ifExists) {
        this.configName = configName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.DROP_SCHEDULER_CONFIG;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public String getConfigName() {
        return configName;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

}
