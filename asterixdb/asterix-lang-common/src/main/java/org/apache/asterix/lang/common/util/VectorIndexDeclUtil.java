package org.apache.asterix.lang.common.util;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class VectorIndexDeclUtil {
    /* ***********************************************
     * Vector Index Policy Parameters
     * ***********************************************
     */
    public static final String VECTOR_INDEX_PARAMETER_DIMENSION = "dimension";
    public static final String VECTOR_INDEX_PARAMETER_DESCRIPTION = "description";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST = "train_list";
    public static final String VECTOR_INDEX_PARAMETER_SIMILARITY = "similarity";
    public static final String VECTOR_INDEX_PARAMETER_NUM_K = "num_k";

    private static final ARecordType WITH_OBJECT_TYPE = getWithObjectType();

    private VectorIndexDeclUtil() {
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord) throws CompilationException {
        if (withRecord == null) {
            return null;
        }
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(withRecord);
        validator.validateType(WITH_OBJECT_TYPE, node);
        return node;
    }

    private static ARecordType getWithObjectType() {
        final String[] withNames = { VECTOR_INDEX_PARAMETER_DIMENSION, VECTOR_INDEX_PARAMETER_DESCRIPTION,
                VECTOR_INDEX_PARAMETER_TRAIN_LIST, VECTOR_INDEX_PARAMETER_SIMILARITY, VECTOR_INDEX_PARAMETER_NUM_K };
        final IAType[] withTypes = { BuiltinType.AINT64, AUnionType.createUnknownableType(BuiltinType.ASTRING),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ASTRING),
                AUnionType.createUnknownableType(BuiltinType.AINT64) };
        return new ARecordType("withObject", withNames, withTypes, false);
    }

}
