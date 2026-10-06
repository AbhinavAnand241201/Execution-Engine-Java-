# State machines

> Generated from `TaskStateMachine` and `WorkflowStateMachine` by `StateMachinesDocument`. Do not edit by hand:
> the build fails (T03.5) if this file and the code disagree.

## Task execution

FAILED is not a task state: a failed attempt is recorded as an attempt outcome and the task moves straight to
RETRYING or FAILED_PERMANENTLY in the same transaction.

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> READY
    PENDING --> CANCELLED
    READY --> QUEUED
    READY --> CANCELLED
    QUEUED --> RUNNING
    QUEUED --> CANCELLED
    RUNNING --> RETRYING
    RUNNING --> SUCCEEDED
    RUNNING --> FAILED_PERMANENTLY
    RUNNING --> CANCELLED
    RETRYING --> READY
    RETRYING --> CANCELLED
    FAILED_PERMANENTLY --> READY
    SUCCEEDED --> [*]
    CANCELLED --> [*]
```

## Workflow execution

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> RUNNING
    RUNNING --> PAUSED
    RUNNING --> CANCELLING
    RUNNING --> COMPLETED
    RUNNING --> FAILED
    PAUSED --> RUNNING
    PAUSED --> CANCELLING
    PAUSED --> COMPLETED
    PAUSED --> FAILED
    CANCELLING --> CANCELLED
    FAILED --> RUNNING
    FAILED --> CANCELLED
    COMPLETED --> [*]
    CANCELLED --> [*]
```

## Attempt outcomes

`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `WORKER_LOST`, `WORKER_SHUTDOWN`, `CANCELLED`. `TIMED_OUT`, `WORKER_LOST`
and `WORKER_SHUTDOWN` are always retryable; `FAILED` is retryable if the executor says so.
