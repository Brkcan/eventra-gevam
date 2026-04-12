package com.eventra.pluginrunner.api;

import java.util.Collections;
import java.util.Map;

public final class PluginResult {
  private final boolean success;
  private final String status;
  private final String message;
  private final Map<String, Object> output;

  public PluginResult(boolean success, String status, String message, Map<String, Object> output) {
    this.success = success;
    this.status = status == null ? (success ? "ok" : "error") : status;
    this.message = message == null ? "" : message;
    this.output = output == null ? Collections.emptyMap() : output;
  }

  public static PluginResult ok(String message, Map<String, Object> output) {
    return new PluginResult(true, "ok", message, output);
  }

  public static PluginResult failed(String message, Map<String, Object> output) {
    return new PluginResult(false, "error", message, output);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public Map<String, Object> getOutput() {
    return output;
  }
}

