package com.flowforge.orchestrator.application;

import com.flowforge.core.error.ErrorCode;
import java.util.UUID;

/**
 * The resource doesn't exist <em>or belongs to someone else</em>; both cases produce the same 404, so a caller can't
 * learn which ids exist. The id is not echoed in the message for the same reason.
 */
public class NotFoundException extends ApplicationException {

    public NotFoundException(String resource, UUID id) {
        super(ErrorCode.NOT_FOUND, "No such " + resource);
    }
}
