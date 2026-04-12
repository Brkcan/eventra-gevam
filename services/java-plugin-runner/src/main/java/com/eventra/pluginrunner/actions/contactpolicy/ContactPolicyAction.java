package com.eventra.pluginrunner.actions.contactpolicy;

import com.eventra.pluginrunner.actions.contactpolicy.mapper.ContactPolicyMapper;
import com.eventra.pluginrunner.actions.contactpolicy.model.ContactPolicyInput;
import com.eventra.pluginrunner.actions.contactpolicy.model.ContactPolicyOutput;
import com.eventra.pluginrunner.actions.contactpolicy.service.ContactPolicyClientConfig;
import com.eventra.pluginrunner.actions.contactpolicy.service.ContactPolicyServiceClient;
import com.eventra.pluginrunner.actions.contactpolicy.service.ContactPolicyServiceResponse;
import com.eventra.pluginrunner.api.JourneyActionPlugin;
import com.eventra.pluginrunner.api.PluginExecution;
import com.eventra.pluginrunner.api.PluginResult;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ContactPolicyAction implements JourneyActionPlugin {
  private final ContactPolicyMapper mapper;
  private final ContactPolicyServiceClient serviceClient;

  public ContactPolicyAction() {
    this(new ContactPolicyMapper(), new ContactPolicyServiceClient());
  }

  ContactPolicyAction(ContactPolicyMapper mapper, ContactPolicyServiceClient serviceClient) {
    this.mapper = mapper;
    this.serviceClient = serviceClient;
  }

  @Override
  public String id() {
    return "eventra.contact.policy.action.v1";
  }

  @Override
  public String displayName() {
    return "ContactPolicyAction";
  }

  @Override
  public String description() {
    return "Contact policy servisine POST atar. Input/Output mapping kod tarafinda sabittir.";
  }

  @Override
  public Map<String, Object> inputSchema() {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("customer_id", "string");
    schema.put("event_id", "string");
    schema.put("event_type", "string");
    schema.put("journey_id", "string");
    schema.put("journey_version", "number");
    schema.put("payload", "object");
    schema.put("attributes", "object");
    return schema;
  }

  @Override
  public Map<String, Object> outputSchema() {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("contact_allowed", "boolean");
    schema.put("policy_reason", "string");
    schema.put("raw_response", "object");
    schema.put("status_code", "number");
    return schema;
  }

  @Override
  public PluginResult execute(PluginExecution execution) throws Exception {
    String endpoint = String.valueOf(System.getenv().getOrDefault("CONTACT_POLICY_URL", "")).trim();
    if (endpoint.isEmpty()) {
      return PluginResult.failed(
          "CONTACT_POLICY_URL missing",
          Map.of("status_code", 0, "contact_allowed", false, "policy_reason", "endpoint_missing"));
    }

    int timeoutMs =
        Math.max(
            500,
            Integer.parseInt(System.getenv().getOrDefault("CONTACT_POLICY_TIMEOUT_MS", "5000")));
    String bearerToken = String.valueOf(System.getenv().getOrDefault("CONTACT_POLICY_BEARER_TOKEN", ""));
    ContactPolicyClientConfig config = new ContactPolicyClientConfig(endpoint, timeoutMs, bearerToken);

    ContactPolicyInput input = mapper.toInput(execution);
    ContactPolicyServiceResponse serviceResponse = serviceClient.checkPolicy(input, config);
    int statusCode = serviceResponse.statusCode();
    Map<String, Object> responseBody = serviceResponse.body();
    boolean ok = statusCode >= 200 && statusCode < 300;

    ContactPolicyOutput output = mapper.toOutput(input, statusCode, responseBody);
    Map<String, Object> outputPayload = mapper.toPluginOutputMap(output);

    if (!ok) {
      return PluginResult.failed("contact_policy_http_" + statusCode, outputPayload);
    }
    return PluginResult.ok("contact_policy_ok", outputPayload);
  }
}
