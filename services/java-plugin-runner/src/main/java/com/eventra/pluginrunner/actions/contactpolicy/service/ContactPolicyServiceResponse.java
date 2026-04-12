package com.eventra.pluginrunner.actions.contactpolicy.service;

import java.util.Map;

public record ContactPolicyServiceResponse(
    int statusCode,
    Map<String, Object> body) {}

