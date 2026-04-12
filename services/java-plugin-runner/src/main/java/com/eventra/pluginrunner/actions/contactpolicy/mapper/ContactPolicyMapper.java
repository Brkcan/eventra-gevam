package com.eventra.pluginrunner.actions.contactpolicy.mapper;

import com.eventra.pluginrunner.actions.contactpolicy.model.ContactPolicyInput;
import com.eventra.pluginrunner.actions.contactpolicy.model.ContactPolicyOutput;
import com.eventra.pluginrunner.api.PluginExecution;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ContactPolicyMapper {
  public ContactPolicyInput toInput(PluginExecution execution) {
    Map<String, Object> event = asMap(execution.getEvent());
    Map<String, Object> journey = asMap(execution.getJourney());
    Map<String, Object> context = asMap(execution.getContext());
    Map<String, Object> payload = asMap(event.get("payload"));
    Map<String, Object> attributes = asMap(resolvePath(context, "attributes"));

    return new ContactPolicyInput(
        stringValue(event.get("customer_id")),
        stringValue(event.get("event_id")),
        stringValue(event.get("event_type")),
        stringValue(journey.get("journey_id")),
        intValue(journey.get("version"), 1),
        payload,
        attributes);
  }

  public ContactPolicyOutput toOutput(
      ContactPolicyInput input,
      int statusCode,
      Map<String, Object> responseBody) {
    Object directAllowed = responseBody.get("contact_allowed");
    Object nestedAllowed = resolvePath(responseBody, "policy.allowed");
    Object resolvedAllowed = directAllowed != null ? directAllowed : nestedAllowed;

    Object directReason = responseBody.get("policy_reason");
    Object nestedReason = resolvePath(responseBody, "policy.reason");
    Object resolvedReason = directReason != null ? directReason : nestedReason;

    return new ContactPolicyOutput(
        toBoolean(resolvedAllowed),
        resolvedReason == null ? "" : String.valueOf(resolvedReason),
        responseBody,
        statusCode,
        Map.of(
            "customer_id", input.customerId(),
            "event_id", input.eventId(),
            "event_type", input.eventType(),
            "journey_id", input.journeyId(),
            "journey_version", input.journeyVersion(),
            "payload", input.payload(),
            "attributes", input.attributes()));
  }

  public Map<String, Object> toPluginOutputMap(ContactPolicyOutput output) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("contact_allowed", output.contactAllowed());
    map.put("policy_reason", output.policyReason());
    map.put("raw_response", output.rawResponse());
    map.put("status_code", output.statusCode());
    map.put("mapped_input", output.mappedInput());
    return map;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> asMap(Object value) {
    if (!(value instanceof Map<?, ?> mapValue)) {
      return new LinkedHashMap<>();
    }
    return (Map<String, Object>) mapValue;
  }

  private Object resolvePath(Map<String, Object> root, String dottedPath) {
    String path = String.valueOf(dottedPath == null ? "" : dottedPath).trim();
    if (path.isEmpty()) {
      return null;
    }
    String[] parts = path.split("\\.");
    Object cursor = root;
    for (String part : parts) {
      if (!(cursor instanceof Map<?, ?> mapCursor)) {
        return null;
      }
      cursor = mapCursor.get(part);
      if (cursor == null) {
        return null;
      }
    }
    return cursor;
  }

  private String stringValue(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  private int intValue(Object value, int fallback) {
    if (value instanceof Number numberValue) {
      return numberValue.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (Exception ignored) {
      return fallback;
    }
  }

  private boolean toBoolean(Object value) {
    if (value instanceof Boolean boolValue) {
      return boolValue;
    }
    if (value instanceof Number numberValue) {
      return numberValue.intValue() != 0;
    }
    String raw = String.valueOf(value == null ? "" : value).trim().toLowerCase();
    return "true".equals(raw) || "1".equals(raw) || "yes".equals(raw);
  }
}

