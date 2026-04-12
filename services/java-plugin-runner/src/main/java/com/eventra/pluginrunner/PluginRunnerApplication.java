package com.eventra.pluginrunner;

import com.eventra.pluginrunner.api.PluginResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public final class PluginRunnerApplication {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private PluginRunnerApplication() {}

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("JAVA_PLUGIN_RUNNER_PORT", "3030"));
    String pluginDirRaw = System.getenv().getOrDefault("JAVA_PLUGIN_DIR", "plugins");
    Path pluginDir = Path.of(pluginDirRaw).toAbsolutePath();

    PluginRegistry registry = new PluginRegistry(pluginDir);
    registry.reload();

    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext("/health", exchange -> handleHealth(exchange, pluginDir));
    server.createContext("/plugins/actions", exchange -> handleListPlugins(exchange, registry));
    server.createContext("/plugins/reload", exchange -> handleReload(exchange, registry));
    server.createContext("/plugins/execute", exchange -> handleExecute(exchange, registry));
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();

    System.out.println(
        "[java-plugin-runner] listening on :" + port + " plugin_dir=" + pluginDir.toString());
  }

  private static void handleHealth(HttpExchange exchange, Path pluginDir) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respondJson(exchange, 405, Map.of("status", "error", "message", "method_not_allowed"));
      return;
    }
    respondJson(
        exchange,
        200,
        Map.of("status", "ok", "service", "java-plugin-runner", "plugin_dir", pluginDir.toString()));
  }

  private static void handleListPlugins(HttpExchange exchange, PluginRegistry registry)
      throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respondJson(exchange, 405, Map.of("status", "error", "message", "method_not_allowed"));
      return;
    }
    respondJson(exchange, 200, Map.of("status", "ok", "items", registry.listActions()));
  }

  private static void handleReload(HttpExchange exchange, PluginRegistry registry) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respondJson(exchange, 405, Map.of("status", "error", "message", "method_not_allowed"));
      return;
    }
    try {
      registry.reload();
      respondJson(exchange, 200, Map.of("status", "ok", "items", registry.listActions()));
    } catch (Exception error) {
      respondJson(exchange, 500, Map.of("status", "error", "message", error.getMessage()));
    }
  }

  private static void handleExecute(HttpExchange exchange, PluginRegistry registry) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respondJson(exchange, 405, Map.of("status", "error", "message", "method_not_allowed"));
      return;
    }
    try {
      Map<String, Object> body =
          MAPPER.readValue(exchange.getRequestBody().readAllBytes(), MAP_TYPE);
      String pluginId = String.valueOf(body.getOrDefault("plugin_id", "")).trim();
      Map<String, Object> actionConfig = asMap(body.get("action_config"));
      Map<String, Object> event = asMap(body.get("event"));
      Map<String, Object> journey = asMap(body.get("journey"));
      Map<String, Object> instance = asMap(body.get("instance"));
      Map<String, Object> context = asMap(body.get("context"));

      if (pluginId.isEmpty()) {
        respondJson(exchange, 400, Map.of("status", "error", "message", "plugin_id_required"));
        return;
      }

      PluginResult result = registry.execute(pluginId, actionConfig, event, journey, instance, context);
      Map<String, Object> payload = new HashMap<>();
      payload.put("status", result.getStatus());
      payload.put("success", result.isSuccess());
      payload.put("message", result.getMessage());
      payload.put("output", result.getOutput());
      respondJson(exchange, result.isSuccess() ? 200 : 422, payload);
    } catch (Exception error) {
      respondJson(exchange, 500, Map.of("status", "error", "message", error.getMessage()));
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    if (!(value instanceof Map<?, ?> mapValue)) {
      return Collections.emptyMap();
    }
    return (Map<String, Object>) mapValue;
  }

  private static void respondJson(HttpExchange exchange, int statusCode, Object payload)
      throws IOException {
    byte[] bytes = MAPPER.writeValueAsBytes(payload);
    exchange.getResponseHeaders().set("content-type", "application/json; charset=utf-8");
    exchange.sendResponseHeaders(statusCode, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.getResponseBody().close();
    exchange.close();
  }
}
