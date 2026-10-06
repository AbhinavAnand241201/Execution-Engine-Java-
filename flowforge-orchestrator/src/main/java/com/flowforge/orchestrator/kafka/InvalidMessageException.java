package com.flowforge.orchestrator.kafka;

/** A record that can never be processed (not valid JSON for its type): retrying is pointless, it goes to the DLT. */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
