package com.eventra.pluginrunner.api;

import java.util.Collections;
import java.util.Map;

public final class PluginExecution {
  private final Map<String, Object> actionConfig;
  private final Map<String, Object> event;
  private final Map<String, Object> journey;
  private final Map<String, Object> instance;
  private final Map<String, Object> context;

  public PluginExecution(
      Map<String, Object> actionConfig,
      Map<String, Object> event,
      Map<String, Object> journey,
      Map<String, Object> instance,
      Map<String, Object> context) {
    this.actionConfig = actionConfig == null ? Collections.emptyMap() : actionConfig;
    this.event = event == null ? Collections.emptyMap() : event;
    this.journey = journey == null ? Collections.emptyMap() : journey;
    this.instance = instance == null ? Collections.emptyMap() : instance;
    this.context = context == null ? Collections.emptyMap() : context;
  }

  public Map<String, Object> getActionConfig() {
    return actionConfig;
  }

  public Map<String, Object> getEvent() {
    return event;
  }

  public Map<String, Object> getJourney() {
    return journey;
  }

  public Map<String, Object> getInstance() {
    return instance;
  }

  public Map<String, Object> getContext() {
    return context;
  }
}

