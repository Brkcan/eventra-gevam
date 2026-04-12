package com.eventra.pluginrunner.actions.contactpolicy.service;

public record ContactPolicyClientConfig(
    String endpoint,
    int timeoutMs,
    String bearerToken) {}

