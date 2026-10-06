package com.flowforge.core.state;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import java.util.UUID;

/**
 * A requested status change that the state machine doesn't allow, e.g. pausing a COMPLETED execution. Carries the
 * entity and both states so logs and the 409 response say exactly what was attempted. The id is a server-generated
 * UUID, so nothing user-supplied is echoed into the client-facing message.
 */
public class IllegalStateTransitionException extends FlowForgeException {

    private final String entityType;
    private final UUID entityId;
    private final Enum<?> from;
    private final Enum<?> to;

    public IllegalStateTransitionException(String entityType, UUID entityId, Enum<?> from, Enum<?> to) {
        super(ErrorCode.ILLEGAL_STATE_TRANSITION,
            "Cannot move " + entityType + " " + entityId + " from " + from + " to " + to);
        this.entityType = entityType;
        this.entityId = entityId;
        this.from = from;
        this.to = to;
    }

    public String entityType() {
        return entityType;
    }

    public UUID entityId() {
        return entityId;
    }

    public Enum<?> from() {
        return from;
    }

    public Enum<?> to() {
        return to;
    }
}
