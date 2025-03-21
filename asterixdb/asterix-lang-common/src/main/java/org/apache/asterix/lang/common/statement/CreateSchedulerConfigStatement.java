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
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.SchedulerConfigUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateSchedulerConfigStatement extends AbstractStatement {

    private final Namespace namespace;
    private final String configName;
    private final boolean ifNotExists;
    private final AdmObjectNode configNode;

    public static final String FIELD_NAME_DEFAULT_PRIORITY = "defaultPriority";
    public static final String FIELD_NAME_SHORT_MEMORY_QUOTA = "shortMemoryPercent";
    public static final String FIELD_NAME_SHORT_CPU_QUOTA = "shortCPUQuota";
    public static final String FIELD_NAME_QUERY_GROUP = "queryGroup";
    public static final String FIELD_NAME_QG_PRIORITY = "priority";
    public static final String FIELD_NAME_QG_GROUPLIST = "grouplist";

    public CreateSchedulerConfigStatement(Namespace namespace, String configName, boolean ifNotExists,
            RecordConstructor expr) throws CompilationException {
        this.namespace = namespace;
        this.configName = configName;
        this.ifNotExists = ifNotExists;
        this.configNode = SchedulerConfigUtil.validateAndGetConfigNode(expr);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public String getConfigName() {
        return configName;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_SCHEDULER_CONFIG;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public long getDefaultPriority() {
        AdmBigIntNode admNode = (AdmBigIntNode) configNode.get(FIELD_NAME_DEFAULT_PRIORITY);
        return admNode.get();
    }

    public double getShortMemoryPercent() {
        AdmDoubleNode admNode = (AdmDoubleNode) configNode.get(FIELD_NAME_SHORT_MEMORY_QUOTA);
        return admNode.get();
    }

    public long getShortCPUQuota() {
        AdmBigIntNode admNode = (AdmBigIntNode) configNode.get(FIELD_NAME_SHORT_CPU_QUOTA);
        return admNode.get();
    }

    public Map<Long, List<String>> getQueryGroups() {

        AdmArrayNode arrayNode = (AdmArrayNode) configNode.get(FIELD_NAME_QUERY_GROUP);
        Map<Long, List<String>> priorityToGroup = new HashMap<>();

        for (IAdmNode iAdmNode : arrayNode) {
            AdmObjectNode abn = (AdmObjectNode)iAdmNode;
            AdmBigIntNode abin = (AdmBigIntNode) abn.get(FIELD_NAME_QG_PRIORITY);
            long priority = abin.get();

            List<String> groupNames = new ArrayList<>();
            AdmArrayNode groupListNode = (AdmArrayNode) abn.get(FIELD_NAME_QG_GROUPLIST);
            for(IAdmNode groupName: groupListNode) {
                groupNames.add(((AdmStringNode) groupName).get());
            }

            priorityToGroup.put(priority, groupNames);
        }

        return priorityToGroup;
    }

}