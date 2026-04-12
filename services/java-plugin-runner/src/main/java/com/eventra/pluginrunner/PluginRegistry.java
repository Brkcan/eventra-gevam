package com.eventra.pluginrunner;

import com.eventra.pluginrunner.api.JourneyActionPlugin;
import com.eventra.pluginrunner.api.PluginExecution;
import com.eventra.pluginrunner.api.PluginResult;
import com.eventra.pluginrunner.actions.contactpolicy.ContactPolicyAction;
import com.eventra.pluginrunner.builtin.MockKafkaProducePlugin;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class PluginRegistry {
  private final Path pluginDir;
  private final Map<String, JourneyActionPlugin> plugins = new ConcurrentHashMap<>();

  public PluginRegistry(Path pluginDir) {
    this.pluginDir = pluginDir;
  }

  public synchronized void reload() throws IOException {
    plugins.clear();
    registerBuiltinPlugins();
    if (!Files.exists(pluginDir)) {
      Files.createDirectories(pluginDir);
      return;
    }

    List<Path> jars;
    try (var stream = Files.list(pluginDir)) {
      jars =
          stream
              .filter(path -> path.getFileName().toString().endsWith(".jar"))
              .sorted(Comparator.comparing(path -> path.getFileName().toString()))
              .collect(Collectors.toList());
    }

    for (Path jar : jars) {
      loadPluginsFromJar(jar);
    }
  }

  private void registerBuiltinPlugins() {
    JourneyActionPlugin mockPlugin = new MockKafkaProducePlugin();
    plugins.put(mockPlugin.id(), mockPlugin);

    JourneyActionPlugin contactPolicyAction = new ContactPolicyAction();
    plugins.put(contactPolicyAction.id(), contactPolicyAction);
  }

  private void loadPluginsFromJar(Path jarPath) {
    try {
      URL jarUrl = jarPath.toUri().toURL();
      URLClassLoader classLoader =
          new URLClassLoader(new URL[] {jarUrl}, JourneyActionPlugin.class.getClassLoader());
      ServiceLoader<JourneyActionPlugin> loader =
          ServiceLoader.load(JourneyActionPlugin.class, classLoader);

      for (JourneyActionPlugin plugin : loader) {
        String pluginId = String.valueOf(plugin.id()).trim();
        if (pluginId.isEmpty()) {
          continue;
        }
        plugins.put(pluginId, plugin);
      }
    } catch (Exception error) {
      System.err.println(
          "[java-plugin-runner] failed loading jar " + jarPath + ": " + error.getMessage());
    }
  }

  public List<Map<String, Object>> listActions() {
    List<Map<String, Object>> items = new ArrayList<>();
    for (JourneyActionPlugin plugin : plugins.values()) {
      Map<String, Object> item = new HashMap<>();
      item.put("id", plugin.id());
      item.put("display_name", plugin.displayName());
      item.put("description", plugin.description());
      item.put("config_schema", plugin.configSchema());
      item.put("input_schema", plugin.inputSchema());
      item.put("output_schema", plugin.outputSchema());
      items.add(item);
    }
    items.sort(Comparator.comparing(item -> String.valueOf(item.get("id"))));
    return items;
  }

  public PluginResult execute(
      String pluginId,
      Map<String, Object> actionConfig,
      Map<String, Object> event,
      Map<String, Object> journey,
      Map<String, Object> instance,
      Map<String, Object> context) {
    JourneyActionPlugin plugin = plugins.get(pluginId);
    if (plugin == null) {
      return PluginResult.failed(
          "plugin_not_found: " + pluginId, Map.of("plugin_id", pluginId, "known_plugins", plugins.keySet()));
    }
    try {
      return plugin.execute(new PluginExecution(actionConfig, event, journey, instance, context));
    } catch (Exception error) {
      return PluginResult.failed(
          "plugin_execution_failed: " + error.getMessage(),
          Map.of("plugin_id", pluginId, "error_class", error.getClass().getName()));
    }
  }
}
