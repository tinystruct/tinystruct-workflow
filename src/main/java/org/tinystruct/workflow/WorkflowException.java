package org.tinystruct.workflow;

import org.tinystruct.ApplicationException;

/** Base exception for all workflow engine errors. */
public class WorkflowException extends ApplicationException {
    private static final long serialVersionUID = 1L;

    public WorkflowException(String message) {
        super(message);
    }

    public WorkflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
