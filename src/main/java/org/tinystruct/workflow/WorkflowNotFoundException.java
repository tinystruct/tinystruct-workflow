package org.tinystruct.workflow;

/** Thrown when a workflow definition or execution ID cannot be found. */
public class WorkflowNotFoundException extends WorkflowException {
    private static final long serialVersionUID = 1L;

    public WorkflowNotFoundException(String id) {
        super("Workflow not found: " + id);
    }
}
