package com.eventra.pluginrunner.actions.contactpolicy.model;

import java.util.Map;

public record ContactPolicyInput(
    String customerId,
    String eventId,
    String eventType,
    String journeyId,
    int journeyVersion,
    Map<String, Object> payload,
    Map<String, Object> attributes) {}

