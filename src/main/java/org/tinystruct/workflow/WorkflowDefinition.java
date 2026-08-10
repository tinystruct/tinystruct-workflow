package org.tinystruct.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Describes a workflow as an ordered sequence of action paths.
 * Each node is an action name resolvable by {@link org.tinystruct.system.ApplicationManager}.
 *
 * <pre>
 * WorkflowDefinition def = new WorkflowDefinition("order-process")
 *         .addNode("validate")
 *         .addNode("charge")
 *         .addNode("fulfill");
 * </pre>
 */
public class WorkflowDefinition {
    private final String workflowId;
    private final List<String> nodes = new ArrayList<>();

    public WorkflowDefinition(String workflowId) {
        if (workflowId == null || workflowId.isBlank()) {
            throw new IllegalArgumentException("workflowId must not be blank");
        }
        this.workflowId = workflowId;
    }

    /**
     * Appends a node to this workflow.
     *
     * @param actionPath the action name to invoke at this step
     * @return this definition, for chaining
     */
    public WorkflowDefinition addNode(String actionPath) {
        if (actionPath == null || actionPath.isBlank()) {
            throw new IllegalArgumentException("actionPath must not be blank");
        }
        nodes.add(actionPath);
        return this;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public List<String> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public int size() {
        return nodes.size();
    }

    public String getNode(int index) {
        return nodes.get(index);
    }
}
