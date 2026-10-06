package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeMap;

/**
 * SHA-256 of a request in canonical form (object keys sorted, no whitespace), so {@code {"a":1,"b":2}} and
 * {@code {"b":2, "a":1}} count as the same request when an Idempotency-Key is reused.
 */
public final class RequestHasher {

    private final ObjectMapper json;

    public RequestHasher(ObjectMapper json) {
        this.json = json;
    }

    public String hash(JsonNode request) {
        try {
            byte[] canonical = json.writeValueAsBytes(canonical(request));
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new IllegalStateException("cannot hash request", e);
        }
    }

    private JsonNode canonical(JsonNode node) {
        if (node == null) {
            return json.nullNode();
        }
        if (node.isObject()) {
            TreeMap<String, JsonNode> sorted = new TreeMap<>();
            node.fields().forEachRemaining(field -> sorted.put(field.getKey(), canonical(field.getValue())));
            ObjectNode result = json.createObjectNode();
            sorted.forEach(result::set);
            return result;
        }
        if (node.isArray()) {
            ArrayNode result = json.createArrayNode();
            node.forEach(element -> result.add(canonical(element)));
            return result;
        }
        return node;
    }

    /** UTF-8 size of a JSON value as stored. */
    public int sizeInBytes(JsonNode node) {
        return node == null ? 0 : node.toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
