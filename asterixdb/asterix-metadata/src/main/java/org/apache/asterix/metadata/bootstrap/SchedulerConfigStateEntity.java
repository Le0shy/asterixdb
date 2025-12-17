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

package org.apache.asterix.metadata.bootstrap;

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_SCHEDULER_STATE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.*;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class SchedulerConfigStateEntity {
    private static final SchedulerConfigStateEntity SCHEDULER_CONFIG_STATE =
            new SchedulerConfigStateEntity(new MetadataIndex(PROPERTIES_SCHEDULER_STATE, 2,
                    new IAType[] { BuiltinType.ASTRING }, Arrays.asList(List.of(FIELD_NAME_SCHEDULER_CONFIG_NAME)), 0,
                    schedulerConfigStateType(), true, new int[] { 0 }), 1, -1);

    private static ARecordType schedulerConfigStateType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_SCHEDULER_CONFIG_STATE,
                new String[] { FIELD_NAME_SCHEDULER_CONFIG_NAME, FIELD_NAME_SCHEDULER_ENABLED_CONFIG_NAME },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, true);
    }

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int configNameIndex;
    private final int enabledConfigNameIndex;

    private SchedulerConfigStateEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
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

    public int configNameIndex() {
        return configNameIndex;
    }

    public int enabledConfigNameIndex() {
        return enabledConfigNameIndex;
    }
}
