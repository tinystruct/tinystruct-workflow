package org.tinystruct.workflow;

/** Thrown when a snapshot repository fails to read or write an execution context. */
public class SnapshotIOException extends WorkflowException {
    private static final long serialVersionUID = 1L;

    public SnapshotIOException(String message) {
        super(message);
    }

    public SnapshotIOException(String message, Throwable cause) {
        super(message, cause);
    }
}
