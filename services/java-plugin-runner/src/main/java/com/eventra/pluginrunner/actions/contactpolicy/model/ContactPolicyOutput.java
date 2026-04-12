package com.eventra.pluginrunner.actions.contactpolicy.model;

import java.util.Map;

public record ContactPolicyOutput(
    boolean contactAllowed,
    String policyReason,
    Map<String, Object> rawResponse,
    int statusCode,
    Map<String, Object> mappedInput) {}

