package com.flowforge.orchestrator.application;

/** A signed token and how many seconds it is valid for. */
public record AccessToken(String accessToken, String tokenType, long expiresIn) {
}
