package com.flowforge.orchestrator.application;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;

/** An expected business failure raised by an application service (e.g. EMAIL_TAKEN, WEAK_PASSWORD). */
public class ApplicationException extends FlowForgeException {

    public ApplicationException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
