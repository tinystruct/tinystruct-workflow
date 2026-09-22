# tinystruct-workflow

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://www.oracle.com/java/technologies/downloads/)

A lightweight, stateful, persistent, and event-driven workflow engine module designed specifically for the **tinystruct** framework.

`tinystruct-workflow` allows you to model workflows as an ordered sequence of executable action paths (nodes), persist execution snapshots across restarts/nodes, and seamlessly pause execution to wait for external events (such as user approvals, webhooks, or timers).

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture & Core Concepts](#architecture--core-concepts)
- [Workflow Lifecycle States](#workflow-lifecycle-states)
- [Requirements & Installation](#requirements--installation)
- [Quick Start Guide](#quick-start-guide)
  - [1. Register Application Actions](#1-register-application-actions)
  - [2. Define the Workflow](#2-define-the-workflow)
  - [3. Select a Snapshot Repository](#3-select-a-snapshot-repository)
  - [4. Run the Workflow Engine](#4-run-the-workflow-engine)
- [Event-Driven Suspend & Resume](#event-driven-suspend--resume)
  - [Declaring custom Workflow Events](#declaring-custom-workflow-events)
  - [Suspending within an Action](#suspending-within-an-action)
  - [Resuming automatically via EventDispatcher](#resuming-automatically-via-eventdispatcher)
  - [Resuming manually](#resuming-manually)
- [Pluggable Persistence (SnapshotRepositories)](#pluggable-persistence-snapshotrepositories)
  - [MemorySnapshotRepository](#memorysnapshotrepository)
  - [FileSnapshotRepository](#filesnapshotrepository)
  - [RedisSnapshotRepository](#redissnapshotrepository)
  - [DatabaseSnapshotRepository](#databasesnapshotrepository)
- [Concurrency Safety](#concurrency-safety)
- [License](#license)

---

## Key Features

- **Seamless tinystruct Integration**: Executes standard tinystruct actions as workflow steps via `ApplicationManager.call(actionPath, context)`.
- **Thread-Safe Runtime Execution**: Drives workflows node-by-node with distributed lock safety.
- **Pluggable Persistence**: Out-of-the-box snapshot persistence to Memory, Filesystem, Redis, or JDBC SQL databases.
- **State Serialization Guarantee**: Complete JSON serialization/deserialization on save and load to prevent memory leaks and thread safety violations.
- **Event-Driven Pause & Resume**: Simple `Workflow.suspend(...)` syntax allows workflows to wait for typed events and resume automatically when matching events are dispatched.
- **Flexible Variables Context**: Share state dynamically across steps using `Workflow.setVariable()` and `Workflow.getVariable()`.

---

## Architecture & Core Concepts

```
              +-------------------+
              |  WorkflowEngine   | <--- Entry Point
              +---------+---------+
                        |
                        v
              +-------------------+
              |  ExecutionRuntime |
              +----+----+----+----+
                   |    |    |
      +------------+    |    +------------+
      |                 |                 |
      v                 v                 v
+-----------+    +-------------+    +-----------+
| Snapshot  |    |    Event    |    |   Local   |
|Repository |    | Correlation |    | Thread    |
+-----------+    |  Registry   |    +-----------+
                 +-------------+
```

1. **`WorkflowEngine`**: The main interface for managing the workflow lifecycle. Use it to register workflow definitions, start new executions, resume suspended ones, cancel pending ones, and inspect status.
2. **`WorkflowDefinition`**: Configures the list of step-by-step tinystruct Action paths that make up the workflow.
3. **`ExecutionContext`**: Tracks execution metadata, including `executionId`, current step index, variables, lifecycle status, suspension reasons, and event triggers.
4. **`Workflow`**: Thread-bound static API helper. Within an action, call `Workflow.setVariable()`, `Workflow.getVariable()`, and `Workflow.suspend()` to control and read state.
5. **`SnapshotRepository`**: Handles storing and loading execution states.
6. **`EventCorrelationRegistry`**: Maps dispatched global framework `Event`s to their waiting `executionId`s to resume workflows automatically.

---

## Workflow Lifecycle States

Workflow executions transition through the following lifecycle states:

```
    [ NEW ]
       │
       ▼
  [ RUNNING ] ───(Complete all nodes)───► [ COMPLETED ]
       │
       ├───(Workflow.suspend)───► [ WAITING ] ───(Resume Event)──► [ RUNNING ]
       │                                │
       │                                └───(engine.cancel)──────► [ CANCELLED ]
       │
       └───(Unhandled Exception)────────► [ FAILED ]
```

- **`NEW`**: Execution has been created but not yet processed.
- **`RUNNING`**: Execution is actively calling a step (action node).
- **`WAITING`**: Execution is suspended, waiting for an external event or explicit resume.
- **`COMPLETED`**: Execution finished all steps successfully.
- **`FAILED`**: Execution failed due to an unhandled exception in a step.
- **`CANCELLED`**: Execution was manually cancelled.

---

## Requirements & Installation

- **Java**: 17 or higher
- **tinystruct Framework**: 1.7.34 or higher
- **Lettuce**: 7.6.0.RELEASE (if Redis Snapshot Repository is used)

### Maven Dependency

Add the following to your `pom.xml`:

```xml
<dependency>
    <groupId>org.tinystruct</groupId>
    <artifactId>tinystruct-workflow</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## Quick Start Guide

### 1. Register Application Actions

Workflow steps are regular tinystruct actions. Create an application extending `AbstractApplication` and register it in the `ApplicationManager`:

```java
import org.tinystruct.AbstractApplication;
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.Workflow;

public class OrderApplication extends AbstractApplication {
    @Override
    public void init() {
        setTemplateRequired(false);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Action("order/validate")
    public String validateOrder() {
        String itemId = (String) Workflow.getVariable("itemId");
        System.out.println("Validating order for item: " + itemId);
        
        // Save computed variable for future steps
        Workflow.setVariable("isValid", true);
        return "valid";
    }

    @Action("order/charge")
    public String chargeCard() {
        boolean isValid = (boolean) Workflow.getVariable("isValid");
        if (!isValid) {
            throw new IllegalStateException("Cannot charge invalid order");
        }
        System.out.println("Charging credit card...");
        return "charged";
    }
}
```

### 2. Define the Workflow

Define a sequential workflow by chaining action path nodes:

```java
import org.tinystruct.workflow.WorkflowDefinition;

WorkflowDefinition orderWorkflow = new WorkflowDefinition("order-processing")
        .addNode("order/validate")
        .addNode("order/charge");
```

### 3. Select a Snapshot Repository

Choose how you want workflow state to be persisted:

```java
import org.tinystruct.workflow.repository.MemorySnapshotRepository;
import org.tinystruct.workflow.repository.SnapshotRepository;

// For unit tests / local development
SnapshotRepository repo = new MemorySnapshotRepository();
```

### 4. Run the Workflow Engine

Instantiate the engine, register the workflow, and start it with initial variables:

```java
import org.tinystruct.system.ApplicationManager;
import org.tinystruct.system.Settings;
import org.tinystruct.workflow.WorkflowEngine;
import org.tinystruct.workflow.ExecutionContext;
import java.util.Map;

// 1. Install your application actions in tinystruct
Settings settings = new Settings();
OrderApplication app = new OrderApplication();
ApplicationManager.install(app, settings);

// 2. Setup the engine
WorkflowEngine engine = new WorkflowEngine(repo);
engine.registerWorkflow(orderWorkflow);

// 3. Start execution with initial variables
Map<String, Object> variables = Map.of("itemId", "PROD-9988");
String executionId = engine.start("order-processing", variables);

// 4. Query execution status
ExecutionContext execution = engine.getExecution(executionId);
System.out.println("Execution Status: " + execution.getStatus()); // COMPLETED
```

---

## Event-Driven Suspend & Resume

Workflows can be suspended inside an action step to wait for asynchronous events (e.g., human approval, incoming webhooks, message queue consumer messages).

### Declaring custom Workflow Events

Workflow events must subclass `WorkflowEvent<T>`, where `T` is the type of the payload:

```java
import org.tinystruct.workflow.event.WorkflowEvent;

public class ApprovalEvent extends WorkflowEvent<Boolean> {
    public ApprovalEvent(String executionId, boolean approved) {
        super("ApprovalEvent", executionId, approved);
    }
}
```

### Suspending within an Action

To pause execution, call `Workflow.suspend(reason, EventClass)` inside an action:

```java
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.Workflow;

@Action("order/request-approval")
public String requestApproval() {
    System.out.println("Order requires manual manager approval...");
    
    // Workflow pauses here. An ApprovalEvent must target this execution ID to resume it.
    Workflow.suspend("Waiting for manager approval", ApprovalEvent.class);
    
    return "suspended";
}

@Action("order/fulfill")
public String fulfill() {
    // Get the event payload that resumed this workflow
    Boolean approved = (Boolean) Workflow.getVariable("__resumePayload");
    
    if (approved == null || !approved) {
        throw new RuntimeException("Order rejected by manager.");
    }
    
    System.out.println("Order fulfilled successfully!");
    return "fulfilled";
}
```

### Resuming automatically via EventDispatcher

When an event of the designated type is dispatched globally through `EventDispatcher` and targets the waiting `executionId`, the engine automatically resumes execution at the subsequent node, passing the event payload as `__resumePayload`:

```java
import org.tinystruct.system.EventDispatcher;

// Create and dispatch the event matching the suspended execution's ID
ApprovalEvent approval = new ApprovalEvent(executionId, true);

// This automatically loads execution state, stores true in "__resumePayload", and resumes the workflow
EventDispatcher.getInstance().dispatch(approval);
```

### Resuming manually

If you prefer to manually resume a waiting workflow with or without a payload (or bypass event handlers), use the `WorkflowEngine` directly:

```java
// Resume with no payload
engine.resume(executionId);

// Resume with an event and its associated payload
engine.resume(executionId, new ApprovalEvent(executionId, true));
```

---

## Pluggable Persistence (SnapshotRepositories)

The persistence layer guarantees that if the application crashes or restarts, workflows in a `WAITING` state can be reloaded and resumed instantly from the exact point they paused.

### MemorySnapshotRepository

Stores serialized JSON states in a thread-safe local hash map. Highly useful for unit tests or fast-prototyping, but state is lost when the application process exits.

```java
SnapshotRepository repo = new MemorySnapshotRepository();
```

### FileSnapshotRepository

Persists snapshots to a directory as `<executionId>.json` files. Excellent for single-instance, long-running services.

```java
import org.tinystruct.workflow.repository.FileSnapshotRepository;
import java.nio.file.Paths;

// Saves to "snapshots/" directory
SnapshotRepository repo = new FileSnapshotRepository();

// Custom path
SnapshotRepository customRepo = new FileSnapshotRepository(Paths.get("/var/lib/workflows"));
```

### RedisSnapshotRepository

Uses **Lettuce** to store snapshots in Redis. Ideal for clustered, multi-node, or stateless cloud environments.

Reads configuration automatically from tinystruct's `Settings` (`redis.host`, `redis.port`, `redis.password`). It prefixes keys with `workflow:snapshot:`.

```java
import org.tinystruct.workflow.repository.RedisSnapshotRepository;

RedisSnapshotRepository repo = new RedisSnapshotRepository();

// Close connections on shutdown
repo.close();
```

### DatabaseSnapshotRepository

A robust SQL-backed snapshot repository using JDBC. Supports a broad array of databases (MySQL, PostgreSQL, Oracle, SQLite, H2, etc.).

```java
import org.tinystruct.workflow.repository.DatabaseSnapshotRepository;

DatabaseSnapshotRepository repo = new DatabaseSnapshotRepository();

// Run once at application startup to ensure the workflow_snapshots table is created
repo.initialize();
```

Table schema generated automatically by `initialize()`:
```sql
CREATE TABLE IF NOT EXISTS workflow_snapshots (
  execution_id       VARCHAR(36)  PRIMARY KEY,
  workflow_id        VARCHAR(128) NOT NULL,
  node_index         INT          NOT NULL,
  status             VARCHAR(16)  NOT NULL,
  variables          TEXT,
  suspend_reason     TEXT,
  waiting_event_type VARCHAR(256),
  failure_message    TEXT,
  created_time       BIGINT,
  updated_time       BIGINT
);
```

---

## Concurrency Safety

To prevent multiple threads or clustered nodes from concurrently executing the same workflow instance, the engine leverages tinystruct's distributed synchronization engine:

- Resuming uses `DistributedLock` under the hood via key `"wf:" + executionId`.
- This ensures only one process/thread executes a specific workflow execution at any given moment, safeguarding your database and filesystem snapshots from race conditions.

---

## License

Distributed under the **Apache 2.0 License**. See `LICENSE` for more details.
