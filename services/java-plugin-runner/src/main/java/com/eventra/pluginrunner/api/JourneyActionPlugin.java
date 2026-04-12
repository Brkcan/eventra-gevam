package com.eventra.pluginrunner.api;

import java.util.Collections;
import java.util.Map;

public interface JourneyActionPlugin {
  String id();

  String displayName();

  default String description() {
    return "";
  }

  default Map<String, Object> configSchema() {
    return Collections.emptyMap();
  }

  default Map<String, Object> inputSchema() {
    return Collections.emptyMap();
  }

  default Map<String, Object> outputSchema() {
    return Collections.emptyMap();
  }

  PluginResult execute(PluginExecution execution) throws Exception;
}
