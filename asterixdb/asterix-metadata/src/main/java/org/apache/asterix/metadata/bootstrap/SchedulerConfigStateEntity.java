package org.apache.asterix.metadata.bootstrap;

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_SCHEDULER_STATE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.*;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.*;

public class SchedulerConfigStateEntity {
    private static final SchedulerConfigStateEntity SCHEDULER_CONFIG_STATE = new SchedulerConfigStateEntity(
            new MetadataIndex(PROPERTIES_SCHEDULER_STATE, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_SCHEDULER_CONFIG_NAME)), 0,
                    schedulerConfigStateType(), true, new int[] { 0, 1 }),
            2, -1);

    private static ARecordType schedulerConfigStateType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_SCHEDULER_CONFIG_STATE,
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_SCHEDULER_CONFIG_NAME,
                        FIELD_NAME_SCHEDULER_ENABLED_CONFIG_NAME },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING }, true);
    }

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int configNameIndex;
    private final int enabledConfigNameIndex;

    private SchedulerConfigStateEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.configNameIndex = startIndex++;
        this.enabledConfigNameIndex = startIndex;
    }

    public static SchedulerConfigStateEntity of(boolean usingDatabase) {
        return SCHEDULER_CONFIG_STATE;
    }

    public int payloadPosition() {
        return payloadPosition;
    }

    public ARecordType getRecordType() {
        return index.getPayloadRecordType();
    }

    public MetadataIndex getIndex() {
        return index;
    }

    public int databaseNameIndex() {
        return databaseNameIndex;
    }

    public int dataverseNameIndex() {
        return dataverseNameIndex;
    }

    public int configNameIndex() {
        return configNameIndex;
    }

    public int enabledConfigNameIndex() {
        return enabledConfigNameIndex;
    }
}
