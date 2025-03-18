package org.apache.asterix.metadata.entitytupletranslators;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.bootstrap.SchedulerConfigEntity;
import org.apache.asterix.metadata.entities.SchedulerConfigMetadataEntity;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.scheduler.SchedulerConfigDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE;

public class SchedulerConfigMetadataEntityTupleTranslator extends AbstractTupleTranslator<SchedulerConfigMetadataEntity>{

    private final SchedulerConfigEntity schedulerConfigEntity;
    protected final ArrayTupleReference tuple;

    protected AMutableInt64 aInt64;
    protected AMutableDouble aDouble;

    protected SchedulerConfigMetadataEntityTupleTranslator(boolean getTuple, SchedulerConfigEntity schedulerConfigEntity) {
        super(getTuple, schedulerConfigEntity.getIndex(), schedulerConfigEntity.payloadPosition());
        this.schedulerConfigEntity = schedulerConfigEntity;
        if (getTuple) {
            tuple = new ArrayTupleReference();
            aInt64 = new AMutableInt64(-1);
            aDouble = new AMutableDouble(-1);
        } else {
            tuple = null;
        }
    }

    @Override
    protected SchedulerConfigMetadataEntity createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(
                ((AString) aRecord.getValueByPos(schedulerConfigEntity.dataverseNameIndex())).getStringValue());
        int databaseNameIndex = schedulerConfigEntity.databaseNameIndex();

        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) aRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }

        String name = ((AString) aRecord.getValueByPos(schedulerConfigEntity.configNameIndex())).getStringValue();

        long defaultPriority = ((AInt64)aRecord.getValueByPos(schedulerConfigEntity.defaultPriorityIndex())).getLongValue();

        double shortMemoryPercent = ((ADouble)aRecord.getValueByPos(schedulerConfigEntity.shortMemoryPercentIndex())).getDoubleValue();

        long shortCPUQuota = ((AInt64)aRecord.getValueByPos(schedulerConfigEntity.shortCPUQuotaIndex())).getLongValue();

        IACursor cursor =
                ((AOrderedList) aRecord.getValueByPos(schedulerConfigEntity.queryGroupsIndex())).getCursor();
        Map<String, Long> groupToPriority  = new HashMap<>();

        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            long priority = ((AInt64) field.getValueByPos(0)).getLongValue();

            IACursor groupNamesCursor =
                    ((AOrderedList) (field.getValueByPos(1))).getCursor();
            while (groupNamesCursor.next()) {
                String qgname = ((AString) groupNamesCursor.get()).getStringValue();
                groupToPriority.put(qgname, priority);
            }
        }

        SchedulerConfigDescriptor configDescriptor = new SchedulerConfigDescriptor(databaseName, dataverseName, name,
                defaultPriority, shortMemoryPercent, shortCPUQuota, groupToPriority, true);
        return new SchedulerConfigMetadataEntity(configDescriptor);
    }

    private void writeIndex(String databaseName, String dataverseName, String configName,
            ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        if (schedulerConfigEntity.databaseNameIndex() >= 0) {
            aString.setValue(databaseName);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(configName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(SchedulerConfigMetadataEntity configMetadataEntity)
            throws HyracksDataException {
        tupleBuilder.reset();

        SchedulerConfigDescriptor configDescriptor = configMetadataEntity.getSchedulerConfig();

        writeIndex(configDescriptor.getDatabaseName(), configDescriptor.getDataverseName().getCanonicalForm(),
                configDescriptor.getName(), tupleBuilder);

        recordBuilder.reset(schedulerConfigEntity.getRecordType());

        if (schedulerConfigEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(configDescriptor.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(schedulerConfigEntity.databaseNameIndex(), fieldValue);
        }
        // write dataverse name
        fieldValue.reset();
        aString.setValue(configDescriptor.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigEntity.dataverseNameIndex(), fieldValue);

        // write config name
        fieldValue.reset();
        aString.setValue(configDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigEntity.configNameIndex(), fieldValue);

        // write default priority
        fieldValue.reset();
        aInt64.setValue(configDescriptor.getDefaultPriority());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigEntity.defaultPriorityIndex(), fieldValue);

        // write short memory percent
        fieldValue.reset();
        aDouble.setValue(configDescriptor.getShortMemoryPercent());
        doubleSerde.serialize(aDouble, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigEntity.shortMemoryPercentIndex(), fieldValue);

        // write short CPU quota
        fieldValue.reset();
        aInt64.setValue(configDescriptor.getShortCPUQuota());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigEntity.shortCPUQuotaIndex(), fieldValue);

        // write query groups
        Map<String, Long> groupToPriority = configDescriptor.getGroupToPriority();

        Map<Long, List<String>> priorityToGroup = new HashMap<>();
        for(Map.Entry<String, Long> pair: groupToPriority.entrySet()) {
            String name = pair.getKey();
            long priority = pair.getValue();
            priorityToGroup.putIfAbsent(priority, new ArrayList<>());
            priorityToGroup.get(priority).add(name);
        }

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE, null));

        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (Map.Entry<Long, List<String>> pair : priorityToGroup.entrySet()) {
            long priority = pair.getKey();
            List<String> groupNames = pair.getValue();
            itemValue.reset();
            writeQueryGroupTypeRecord(priority, groupNames, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(schedulerConfigEntity.queryGroupsIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeQueryGroupTypeRecord(long priority, List<String> groupNames, DataOutput out)
            throws HyracksDataException{
        IARecordBuilder qgRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        qgRecordBuilder.reset(MetadataRecordTypes.SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aInt64.setValue(priority);
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        qgRecordBuilder.addField(0, fieldValue);

        // write field 1
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();

        for (String s : groupNames) {
            fieldValue.reset();
            aString.setValue(s);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }

        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        qgRecordBuilder.addField(1, fieldValue);

        qgRecordBuilder.write(out, true);
    }
}
