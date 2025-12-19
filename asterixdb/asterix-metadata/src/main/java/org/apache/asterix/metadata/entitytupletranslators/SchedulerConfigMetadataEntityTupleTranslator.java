/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.metadata.entitytupletranslators;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE;

import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.bootstrap.SchedulerConfigRecordEntity;
import org.apache.asterix.metadata.bootstrap.SchedulerConfigStateEntity;
import org.apache.asterix.metadata.entities.SchedulerConfigMetadataEntity;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.runtime.scheduler.ISchedulerConfigDescriptor;
import org.apache.asterix.runtime.scheduler.SchedulerConfigRecordDescriptor;
import org.apache.asterix.runtime.scheduler.SchedulerConfigStateDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class SchedulerConfigMetadataEntityTupleTranslator
        extends AbstractTupleTranslator<SchedulerConfigMetadataEntity> {

    private final SchedulerConfigRecordEntity schedulerConfigRecordEntity;
    private final SchedulerConfigStateEntity schedulerConfigStateEntity;
    protected final ArrayTupleReference tuple;

    protected AMutableInt64 aInt64;
    protected AMutableDouble aDouble;

    protected SchedulerConfigMetadataEntityTupleTranslator(boolean getTuple,
            SchedulerConfigRecordEntity schedulerConfigRecordEntity) {
        super(getTuple, schedulerConfigRecordEntity.getIndex(), schedulerConfigRecordEntity.payloadPosition());
        this.schedulerConfigRecordEntity = schedulerConfigRecordEntity;
        this.schedulerConfigStateEntity = null;
        if (getTuple) {
            tuple = new ArrayTupleReference();
            aInt64 = new AMutableInt64(-1);
            aDouble = new AMutableDouble(-1);
        } else {
            tuple = null;
        }
    }

    protected SchedulerConfigMetadataEntityTupleTranslator(boolean getTuple,
            SchedulerConfigStateEntity schedulerConfigStateEntity) {
        super(getTuple, schedulerConfigStateEntity.getIndex(), schedulerConfigStateEntity.payloadPosition());
        this.schedulerConfigRecordEntity = null;
        this.schedulerConfigStateEntity = schedulerConfigStateEntity;
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
        if (schedulerConfigRecordEntity != null) {
            return createMetadataEntityRecordFromARecord(aRecord);
        }
        return createMetadatEntityStateFromARecord(aRecord);
    }

    private SchedulerConfigMetadataEntity createMetadataEntityRecordFromARecord(ARecord aRecord) {
        String name = ((AString) aRecord.getValueByPos(schedulerConfigRecordEntity.configNameIndex())).getStringValue();

        long defaultPriority =
                ((AInt64) aRecord.getValueByPos(schedulerConfigRecordEntity.defaultPriorityIndex())).getLongValue();

        double shortMemoryPercent =
                ((ADouble) aRecord.getValueByPos(schedulerConfigRecordEntity.shortMemoryPercentIndex()))
                        .getDoubleValue();

        long shortCPUQuota =
                ((AInt64) aRecord.getValueByPos(schedulerConfigRecordEntity.shortCPUQuotaIndex())).getLongValue();

        IACursor cursor =
                ((AOrderedList) aRecord.getValueByPos(schedulerConfigRecordEntity.queryGroupsIndex())).getCursor();
        Map<String, Long> groupToPriority = new HashMap<>();

        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            String qgname = ((AString) field.getValueByPos(0)).getStringValue();
            long priority = ((AInt64) field.getValueByPos(1)).getLongValue();
            groupToPriority.put(qgname, priority);
        }

        ISchedulerConfigDescriptor configDescriptor = new SchedulerConfigRecordDescriptor(name, defaultPriority,
                shortMemoryPercent, shortCPUQuota, groupToPriority, true);
        return new SchedulerConfigMetadataEntity(configDescriptor);
    }

    private SchedulerConfigMetadataEntity createMetadatEntityStateFromARecord(ARecord aRecord) {
        String configName =
                ((AString) aRecord.getValueByPos(schedulerConfigStateEntity.configNameIndex())).getStringValue();
        String enabledConfigName =
                ((AString) aRecord.getValueByPos(schedulerConfigStateEntity.enabledConfigNameIndex())).getStringValue();

        ISchedulerConfigDescriptor configDescriptor = new SchedulerConfigStateDescriptor(configName, enabledConfigName);
        return new SchedulerConfigMetadataEntity(configDescriptor);
    }

    private void writeIndex(String configName, ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        aString.setValue(configName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(SchedulerConfigMetadataEntity configMetadataEntity)
            throws HyracksDataException {
        ISchedulerConfigDescriptor configDescriptor = configMetadataEntity.getSchedulerConfig();
        if (configDescriptor instanceof SchedulerConfigRecordDescriptor) {
            return getTupleFromMetadataEntityRecord(configMetadataEntity);
        }
        return getTupleFromMetadataEntityState(configMetadataEntity);
    }

    private ITupleReference getTupleFromMetadataEntityState(SchedulerConfigMetadataEntity configMetadataEntity)
            throws HyracksDataException {
        SchedulerConfigStateDescriptor configDescriptor =
                (SchedulerConfigStateDescriptor) configMetadataEntity.getSchedulerConfig();
        tupleBuilder.reset();
        writeIndex(configDescriptor.getName(), tupleBuilder);
        /* write the payload in the second field of the tuple */
        recordBuilder.reset(schedulerConfigStateEntity.getRecordType());

        // write config name
        fieldValue.reset();
        aString.setValue(configDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigStateEntity.configNameIndex(), fieldValue);

        // write enabled config name
        fieldValue.reset();
        aString.setValue(configDescriptor.getEnabledConfigName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigStateEntity.enabledConfigNameIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private ITupleReference getTupleFromMetadataEntityRecord(SchedulerConfigMetadataEntity configMetadataEntity)
            throws HyracksDataException {
        SchedulerConfigRecordDescriptor configDescriptor =
                (SchedulerConfigRecordDescriptor) configMetadataEntity.getSchedulerConfig();
        tupleBuilder.reset();
        writeIndex(configDescriptor.getName(), tupleBuilder);
        /* write the payload in the second field of the tuple */
        recordBuilder.reset(schedulerConfigRecordEntity.getRecordType());

        // write config name
        fieldValue.reset();
        aString.setValue(configDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigRecordEntity.configNameIndex(), fieldValue);

        // write default priority
        fieldValue.reset();
        aInt64.setValue(configDescriptor.getDefaultPriority());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigRecordEntity.defaultPriorityIndex(), fieldValue);

        // write short memory percent
        fieldValue.reset();
        aDouble.setValue(configDescriptor.getShortMemoryPercent());
        doubleSerde.serialize(aDouble, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigRecordEntity.shortMemoryPercentIndex(), fieldValue);

        // write short CPU quota
        fieldValue.reset();
        aInt64.setValue(configDescriptor.getShortCPUQuota());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(schedulerConfigRecordEntity.shortCPUQuotaIndex(), fieldValue);

        // write query groups
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();

        for (Map.Entry<String, Long> pair : configDescriptor.getGroupToPriority().entrySet()) {
            long priority = pair.getValue();
            String qgName = pair.getKey();
            itemValue.reset();
            writeQueryGroupTypeRecord(priority, qgName, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(schedulerConfigRecordEntity.queryGroupsIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeQueryGroupTypeRecord(long priority, String qgName, DataOutput out) throws HyracksDataException {
        IARecordBuilder qgRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        qgRecordBuilder.reset(MetadataRecordTypes.SCHEDULER_COFING_QUERY_GROUPS_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(qgName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        qgRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aInt64.setValue(priority);
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        qgRecordBuilder.addField(1, fieldValue);

        qgRecordBuilder.write(out, true);
    }
}
