package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.VectorIndexDeclUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;

import java.util.List;

public class CreateVectorIndexStatement extends AbstractStatement {

    private final Namespace namespace;
    private final Identifier vectorIndexName;
    private final Identifier datasetName;
    private final CreateIndexStatement.IndexedElement vectorField;
    private final List<CreateIndexStatement.IndexedElement> includedFields;
    private final AdmObjectNode withObjectNode;
    private final boolean ifNotExists;

    public CreateVectorIndexStatement(Namespace namespace, Identifier datasetName, Identifier vectorIndexName,
            CreateIndexStatement.IndexedElement vectorField, List<CreateIndexStatement.IndexedElement> includedFields,
            RecordConstructor withRecord, boolean ifNotExists)
            throws CompilationException {
        this.namespace = namespace;
        this.vectorIndexName = vectorIndexName;
        this.datasetName = datasetName;
        this.vectorField = vectorField;
        this.includedFields = includedFields;
        this.withObjectNode = VectorIndexDeclUtil.validateAndGetWithObjectNode(withRecord);
        this.ifNotExists = ifNotExists;
    }

    public Identifier getVectorIndexName() {
        return vectorIndexName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public Identifier getDatasetName() {
        return datasetName;
    }
    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_VECTOR_INDEX;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

//    public static final class IncludedFields {
//        private final String fieldName;
//        private final String fieldType;
//
//        public IncludedFields(String fieldName, String fieldType) {
//            this.fieldName = fieldName;
//            this.fieldType = fieldType;
//        }
//
//        public String getFieldName() {
//            return fieldName;
//        }
//
//        public String getFieldType() {
//            return fieldType;
//        }
}
