package com.example.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Entry point and implementation of the MCP Server.
 * Uses Spring AI's ToolCallback system to dynamically handle tool requests.
 * Refactored to use Java 17 Records and standard Java logging (No Lombok).
 */
@SpringBootApplication
@RestController
@RequestMapping("/mcp")
public class McpServerApplication {

    private static final Logger log = LoggerFactory.getLogger(McpServerApplication.class);

    private final ObjectMapper mapper;
    
    // Spring AI automatically detects methods annotated with @Tool and registers them as ToolCallback beans
    private final List<ToolCallback> toolCallbacks;

    public McpServerApplication(ObjectMapper mapper, List<ToolCallback> toolCallbacks) {
        this.mapper = mapper;
        this.toolCallbacks = toolCallbacks;
    }

    public static void main(String[] args) {
        SpringApplication.run(McpServerApplication.class, args);
    }

    /**
     * MCP SSE Endpoint for Server-to-Client communication.
     */
    @GetMapping(path = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> sseConnection() {
        log.info("New MCP Client connected via mTLS protected SSE stream.");
        
        ServerSentEvent<String> endpointEvent = ServerSentEvent.<String>builder()
                .event("endpoint")
                .data("/mcp/messages")
                .build();

        Flux<ServerSentEvent<String>> heartbeat = Flux.interval(Duration.ofSeconds(15))
                .map(i -> ServerSentEvent.<String>builder().comment("keepalive").build());

        return Flux.concat(Mono.just(endpointEvent), heartbeat);
    }

    /**
     * MCP Message Endpoint (POST).
     */
    @PostMapping("/messages")
    public Mono<JsonRpcResponse> handleMessage(@RequestBody JsonRpcRequest request) {
        // Record accessors do not use 'get' prefix (e.g. request.method() instead of request.getMethod())
        log.debug("Received MCP Request: method={}, id={}", request.method(), request.id());

        return Mono.fromCallable(() -> processRequest(request))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    private JsonRpcResponse processRequest(JsonRpcRequest req) {
        try {
            switch (req.method()) {
                case "initialize":
                    return success(req.id(), Map.of(
                        "protocolVersion", "0.1.0",
                        "serverInfo", Map.of("name", "java-iceberg-mcp", "version", "1.0"),
                        "capabilities", Map.of("tools", Map.of())
                    ));
                
                case "tools/list":
                    // Dynamically generate the list from Spring AI ToolCallbacks
                    List<Map<String, Object>> tools = toolCallbacks.stream()
                        .map(tool -> Map.of(
                            "name", tool.getToolDefinition().name(),
                            "description", tool.getToolDefinition().description(),
                            "inputSchema", parseSchema(tool.getToolDefinition().inputSchema())
                        ))
                        .collect(Collectors.toList());
                    
                    return success(req.id(), Map.of("tools", tools));

                case "tools/call":
                    Map<String, Object> params = (Map<String, Object>) req.params();
                    String name = (String) params.get("name");
                    Map<String, Object> arguments = (Map<String, Object>) params.get("arguments");

                    // Find the matching Spring AI tool
                    Optional<ToolCallback> toolOptional = toolCallbacks.stream()
                            .filter(t -> t.getToolDefinition().name().equals(name))
                            .findFirst();

                    if (toolOptional.isEmpty()) {
                         throw new IllegalArgumentException("Unknown tool: " + name);
                    }

                    // Execute via Spring AI
                    // Note: We must convert the arguments map back to JSON string for the Spring AI callback
                    String jsonArgs = mapper.writeValueAsString(arguments);
                    String result = toolOptional.get().call(jsonArgs);

                    return success(req.id(), Map.of("content", List.of(Map.of("type", "text", "text", result))));

                default:
                    return success(req.id(), Map.of("status", "ignored"));
            }
        } catch (Exception e) {
            log.error("Error processing request", e);
            return error(req.id(), -32000, e.getMessage());
        }
    }

    // Helper to handle the JSON schema string provided by Spring AI
    private Object parseSchema(String jsonSchema) {
        try {
            return mapper.readValue(jsonSchema, Object.class);
        } catch (JsonProcessingException e) {
            return jsonSchema;
        }
    }

    // --- JSON-RPC DTOs (Java 17 Records) ---

    private JsonRpcResponse success(Object id, Object result) {
        return new JsonRpcResponse("2.0", id, result, null);
    }

    private JsonRpcResponse error(Object id, int code, String message) {
        return new JsonRpcResponse("2.0", id, null, new JsonRpcError(code, message));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record JsonRpcRequest(String jsonrpc, Object id, String method, Object params) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record JsonRpcResponse(String jsonrpc, Object id, Object result, JsonRpcError error) {}

    public record JsonRpcError(int code, String message) {}
}
