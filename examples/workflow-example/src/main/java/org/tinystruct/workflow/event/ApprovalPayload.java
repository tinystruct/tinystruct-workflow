package org.tinystruct.workflow.event;

import java.io.Serializable;

public class ApprovalPayload implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean approved;
    private final String comment;

    public ApprovalPayload(boolean approved, String comment) {
        this.approved = approved;
        this.comment = comment;
    }

    public boolean isApproved() {
        return approved;
    }

    public String getComment() {
        return comment;
    }
}
