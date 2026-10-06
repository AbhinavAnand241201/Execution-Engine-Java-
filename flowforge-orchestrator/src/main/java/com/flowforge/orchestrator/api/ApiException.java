package com.flowforge.orchestrator.api;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;

/** A client error detected by the web layer itself (bad paging or sort parameters, and the like). */
public class ApiException extends FlowForgeException {

    public ApiException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
