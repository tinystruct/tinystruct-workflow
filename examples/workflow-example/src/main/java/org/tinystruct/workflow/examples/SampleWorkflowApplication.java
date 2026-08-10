package org.tinystruct.workflow.examples;

import org.tinystruct.AbstractApplication;
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.Workflow;
import org.tinystruct.workflow.event.ApprovalEvent;
import org.tinystruct.workflow.event.TimerEvent;
import org.tinystruct.workflow.event.WebhookEvent;

public class SampleWorkflowApplication extends AbstractApplication {

    @Override
    public void init() {
        setTemplateRequired(false);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    // --- Approval Workflow ---

    @Action("order/validate")
    public String validateOrder() {
        Workflow.setVariable("orderId", "ORD-12345");
        return "Order validated";
    }

    @Action("order/approve")
    public String requestApproval() {
        Workflow.suspend("Manager approval required", ApprovalEvent.class);
        return "Approval pending";
    }

    @Action("order/fulfill")
    public String fulfillOrder() {
        Object payload = Workflow.getVariable("__resumePayload");
        return "Order fulfilled with payload: " + (payload != null ? payload.toString() : "none");
    }

    // --- Timer Workflow ---

    @Action("job/start")
    public String startJob() {
        Workflow.setVariable("jobId", "JOB-42");
        return "Job started";
    }

    @Action("job/wait")
    public String waitForTimer() {
        Workflow.suspend("Waiting for cooldown", TimerEvent.class);
        return "Timer pending";
    }

    @Action("job/complete")
    public String completeJob() {
        return "Job completed";
    }

    // --- Webhook Workflow ---

    @Action("deploy/prepare")
    public String prepareDeploy() {
        return "Deployment prepared";
    }

    @Action("deploy/wait-confirmation")
    public String waitForWebhook() {
        Workflow.suspend("Waiting for confirmation", WebhookEvent.class);
        return "Webhook pending";
    }

    @Action("deploy/finalize")
    public String finalizeDeploy() {
        return "Deployment finalized";
    }
}
