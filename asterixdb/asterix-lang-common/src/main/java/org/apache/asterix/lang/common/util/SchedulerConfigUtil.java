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
package org.apache.asterix.lang.common.util;

import static org.apache.asterix.lang.common.statement.CreateSchedulerConfigStatement.*;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.*;


public class SchedulerConfigUtil {

    private SchedulerConfigUtil() {
    }

    //--------------------------------------- Scheduler config --------------------------------------//

        /*
        Example of full-text config create statement
        CREATE SCHEDULER CONFIG s_config_1 {
            "defaultPriority": 1,
            "shortMemoryPercent": 10.0,
            "shortCPUQuota": 20,
            "queryGroup":
    	[
            {
                "priority": 4,
                    "grouplist": [ "ui"]
            },

            {
                "priority" : 6,
                    "grouplist": ["analytics"]

            },

            {
                "priority": 8,
                    "grouplist": [ "management" ]
            }
    	]
        };
        */


    private static ARecordType getSchedulerConfigRecordType() {
        /* union type - query group */
        final String[] qgFieldNames = {FIELD_NAME_QG_PRIORITY, FIELD_NAME_QG_GROUPLIST};
        final IAType[] qgFieldTypes = { BuiltinType.AINT64, new AOrderedListType(BuiltinType.ASTRING, null) };
        IAType qgUnionType = new ARecordType("qgRecordType", qgFieldNames, qgFieldTypes, true);

        final String[] fieldNames = { FIELD_NAME_DEFAULT_PRIORITY, FIELD_NAME_SHORT_MEMORY_QUOTA,
                FIELD_NAME_SHORT_CPU_QUOTA, FIELD_NAME_QUERY_GROUP };
        final IAType[] fieldTypes = { BuiltinType.AINT64, BuiltinType.ADOUBLE, BuiltinType.AINT64,
                new AOrderedListType(qgUnionType, null) };

        return new ARecordType("SchedulerConfigRecordType", fieldNames, fieldTypes, true);
    }

    private static final ARecordType SCHEDULER_CONFIG_RECORD_TYPE = getSchedulerConfigRecordType();

    public static AdmObjectNode validateAndGetConfigNode(RecordConstructor recordConstructor)
            throws CompilationException {
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(recordConstructor);
        validator.validateType(SCHEDULER_CONFIG_RECORD_TYPE, node);
        return node;
    }

    public static AdmObjectNode validateUpsertQgroupNode(RecordConstructor recordConstructor)
            throws CompilationException {
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(recordConstructor);
        validator.validateType(SCHEDULER_CONFIG_RECORD_TYPE, node);
        return node;
    }


    public static AdmObjectNode validateDeleteQgroupNode(RecordConstructor recordConstructor)
            throws CompilationException {
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(recordConstructor);
        validator.validateType(SCHEDULER_CONFIG_RECORD_TYPE, node);
        return node;
    }
}