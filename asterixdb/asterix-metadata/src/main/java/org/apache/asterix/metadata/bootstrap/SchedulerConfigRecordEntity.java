package org.apache.asterix.metadata.bootstrap;

import org.apache.asterix.om.types.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_SCHEDULER_CONFIG;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.*;

public class SchedulerConfigRecordEntity {
    private static final SchedulerConfigRecordEntity SCHEDULER_CONFIG = new SchedulerConfigRecordEntity(
            new MetadataIndex(PROPERTIES_SCHEDULER_CONFIG, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_SCHEDULER_CONFIG_NAME)), 0,
                    schedulerConfigRecordType(), true, new int[] { 0, 1 }), 2, -1);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int configNameIndex;
    private final int defaultPriorityIndex;
    private final int shortMemoryPercentIndex;
    private final int shortCPUQuotaIndex;
    private final int queryGroupsIndex;

    private SchedulerConfigRecordEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.configNameIndex = startIndex++;
        this.defaultPriorityIndex = startIndex++;
        this.shortMemoryPercentIndex = startIndex++;
        this.shortCPUQuotaIndex = startIndex++;
        this.queryGroupsIndex = startIndex++;
    }

    public static SchedulerConfigRecordEntity of(boolean usingDatabase) {
        return SCHEDULER_CONFIG;
    }

    public MetadataIndex getIndex() {
        return index;
    }

    public ARecordType getRecordType() {
        return index.getPayloadRecordType();
    }

    public int payloadPosition() {
        return payloadPosition;
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

    public int defaultPriorityIndex() {
        return defaultPriorityIndex;
    }

    public int shortMemoryPercentIndex() {
        return shortMemoryPercentIndex;
    }

    public int shortCPUQuotaIndex() {
        return shortCPUQuotaIndex;
    }

    public int queryGroupsIndex() {
        return queryGroupsIndex;
    }

    private static ARecordType schedulerConfigRecordType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_SCHEDULER_CONFIG,
                new String[] { FIELD_NAME_DATAVERSE_NAME,
                        FIELD_NAME_SCHEDULER_CONFIG_NAME,
                        FIELD_NAME_SCHEDULER_CONFIG_DEFAULT_PRIORITY,
                        FIELD_NAME_SCHEDULER_CONFIG_SHORT_MEMORY_PERCENT,
                        FIELD_NAME_SCHEDULER_CONFIG_SHORT_CPU_QUOTA,
                        FIELD_NAME_SCHEDULER_CONFIG_QUERY_GROUPS },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                        AUnionType.createMissableType(BuiltinType.AINT64),
                        AUnionType.createMissableType(BuiltinType.ADOUBLE),
                        AUnionType.createMissableType(BuiltinType.AINT64),
                        AUnionType.createMissableType(new AOrderedListType(SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE, null)) },
                true);
    }

}
