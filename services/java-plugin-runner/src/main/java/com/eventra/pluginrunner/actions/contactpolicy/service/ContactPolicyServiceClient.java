package com.eventra.pluginrunner.actions.contactpolicy.service;

import com.eventra.pluginrunner.actions.contactpolicy.model.ContactPolicyInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ContactPolicyServiceClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final HttpClient httpClient;

  public ContactPolicyServiceClient() {
    this.httpClient = HttpClient.newBuilder().build();
  }

  public ContactPolicyServiceResponse checkPolicy(
      ContactPolicyInput input,
      ContactPolicyClientConfig config)
      throws Exception {
    Map<String, Object> requestBody = new LinkedHashMap<>();
    requestBody.put("action", "contact_policy_check");
    requestBody.put("input", Map.of(
        "customer_id", input.customerId(),
        "event_id", input.eventId(),
        "event_type", input.eventType(),
        "journey_id", input.journeyId(),
        "journey_version", input.journeyVersion(),
        "payload", input.payload(),
        "attributes", input.attributes()
    ));

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(config.endpoint()))
            .timeout(Duration.ofMillis(config.timeoutMs()))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(requestBody)));

    if (config.bearerToken() != null && !config.bearerToken().isBlank()) {
      requestBuilder.header("authorization", "Bearer " + config.bearerToken());
    }

    HttpResponse<String> response =
        httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    Map<String, Object> responseBody = parseResponseBody(response.body());
    return new ContactPolicyServiceResponse(response.statusCode(), responseBody);
  }

  private Map<String, Object> parseResponseBody(String raw) {
    if (raw == null || raw.isBlank()) {
      return new LinkedHashMap<>();
    }
    try {
      return MAPPER.readValue(raw, MAP_TYPE);
    } catch (Exception ignored) {
      Map<String, Object> fallback = new LinkedHashMap<>();
      fallback.put("raw", raw);
      return fallback;
    }
  }
}

