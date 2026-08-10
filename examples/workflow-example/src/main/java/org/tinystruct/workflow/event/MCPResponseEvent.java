package org.tinystruct.workflow.event;

import org.tinystruct.mcp.JsonRpcResponse;

public class MCPResponseEvent extends WorkflowEvent<JsonRpcResponse> {
    public MCPResponseEvent(String executionId, JsonRpcResponse payload) {
        super("MCPResponseEvent", executionId, payload);
    }
}
