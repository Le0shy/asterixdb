
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class EnableSchedulerStatement extends AbstractStatement {
    private final String configName;

    public EnableSchedulerStatement(String configName) throws CompilationException {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }

    @Override
    public Kind getKind() {
        return Kind.ENABLE_SCHEDULER_CONFIG;
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
