/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.operation.StartWorkflowOperation;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SUB_WORKFLOW;

@Component(TASK_TYPE_SUB_WORKFLOW)
public class SubWorkflow extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubWorkflow.class);
    private static final String SUB_WORKFLOW_ID = "subWorkflowId";

    private final ObjectMapper objectMapper;
    private final StartWorkflowOperation startWorkflowOperation;

    public SubWorkflow(ObjectMapper objectMapper, StartWorkflowOperation startWorkflowOperation) {
        super(TASK_TYPE_SUB_WORKFLOW);
        this.objectMapper = objectMapper;
        this.startWorkflowOperation = startWorkflowOperation;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        Map<String, Object> input = task.getInputData();

        Object subWorkflowNameObj = input.get("subWorkflowName");
        String name;
        if (subWorkflowNameObj != null) {
            name = subWorkflowNameObj.toString();
        } else if (input.get("subWorkflowDefinition") != null) {
            // name will be set from workflowDefinition below
            name = null;
        } else {
            String reason = "Missing subWorkflowName and subWorkflowDefinition in task input";
            LOGGER.error(reason + ": {}", task.getTaskId());
            task.setReasonForIncompletion(reason);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        Object subWorkflowVersionObj = input.get("subWorkflowVersion");
        if (!(subWorkflowVersionObj instanceof Number)) {
            String reason = "Missing or invalid subWorkflowVersion in task input";
            LOGGER.error(reason + ": {}", task.getTaskId());
            task.setReasonForIncompletion(reason);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }
        int version = ((Number) subWorkflowVersionObj).intValue();

        WorkflowDef workflowDefinition = null;
        if (input.get("subWorkflowDefinition") != null) {
            // convert the value back to workflow definition object
            workflowDefinition =
                    objectMapper.convertValue(
                            input.get("subWorkflowDefinition"), WorkflowDef.class);
            name = workflowDefinition.getName();
        }

        Map<String, String> taskToDomain = workflow.getTaskToDomain();
        if (input.get("subWorkflowTaskToDomain") instanceof Map) {
            taskToDomain = (Map<String, String>) input.get("subWorkflowTaskToDomain");
        }

        var wfInput = (Map<String, Object>) input.get("workflowInput");
        if (wfInput == null || wfInput.isEmpty()) {
            wfInput = input;
        }
        String correlationId = workflow.getCorrelationId();

        if (name == null) {
            String reason = "Sub-workflow name is null";
            LOGGER.error(reason + ": {}", task.getTaskId());
            task.setReasonForIncompletion(reason);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        try {
            StartWorkflowInput startWorkflowInput = new StartWorkflowInput();
            startWorkflowInput.setWorkflowDefinition(workflowDefinition);
            startWorkflowInput.setName(name);
            startWorkflowInput.setVersion(version);
            startWorkflowInput.setWorkflowInput(wfInput);
            startWorkflowInput.setCorrelationId(correlationId);
            startWorkflowInput.setParentWorkflowId(workflow.getWorkflowId());
            startWorkflowInput.setParentWorkflowTaskId(task.getTaskId());
            startWorkflowInput.setTaskToDomain(taskToDomain);

            String subWorkflowId = startWorkflowOperation.execute(startWorkflowInput);

            task.setSubWorkflowId(subWorkflowId);
            task.getOutputData().put(SUB_WORKFLOW_ID, subWorkflowId);
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        } catch (Exception e) {
            String reason = "Failed to start sub workflow";
            LOGGER.error(reason + " for task: {}", task.getTaskId(), e);
            task.setReasonForIncompletion(reason + ": " + e.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
        }
    }
}
