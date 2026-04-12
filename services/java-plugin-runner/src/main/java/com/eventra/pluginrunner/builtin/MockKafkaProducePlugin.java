package com.eventra.pluginrunner.builtin;

import com.eventra.pluginrunner.api.JourneyActionPlugin;
import com.eventra.pluginrunner.api.PluginExecution;
import com.eventra.pluginrunner.api.PluginResult;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MockKafkaProducePlugin implements JourneyActionPlugin {
  @Override
  public String id() {
    return "eventra.mock.kafka.produce.v1";
  }

  @Override
  public String displayName() {
    return "Eventra Mock Kafka Producer";
  }

  @Override
  public String description() {
    return "Journey action icinde java_plugin akisini hizli test etmek icin ornek plugin.";
  }

  @Override
  public Map<String, Object> configSchema() {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("bootstrap_servers", "string (ornek: 127.0.0.1:9092)");
    schema.put("topic", "string (ornek: eventra.mock.out)");
    schema.put("key_template", "string (ornek: {{customer_id}})");
    schema.put("payload_template", "json|string");
    return schema;
  }

  @Override
  public PluginResult execute(PluginExecution execution) {
    Map<String, Object> cfg = execution.getActionConfig();
    Map<String, Object> event = execution.getEvent();

    String topic = String.valueOf(cfg.getOrDefault("topic", "eventra.mock.out"));
    String bootstrapServers = String.valueOf(cfg.getOrDefault("bootstrap_servers", "127.0.0.1:9092"));
    String customerId = String.valueOf(event.getOrDefault("customer_id", ""));

    Map<String, Object> output = new LinkedHashMap<>();
    output.put("mode", "mock");
    output.put("topic", topic);
    output.put("bootstrap_servers", bootstrapServers);
    output.put("derived_key", customerId);
    output.put("event_type", String.valueOf(event.getOrDefault("event_type", "")));
    output.put("note", "Bu plugin Kafka'ya gercek produce yapmaz, sadece action akisini test eder.");

    return PluginResult.ok("mock_kafka_produce_ok", output);
  }
}

