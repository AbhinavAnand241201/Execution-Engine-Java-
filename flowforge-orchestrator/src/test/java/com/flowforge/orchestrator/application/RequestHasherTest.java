package com.flowforge.orchestrator.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RequestHasherTest {

    private final ObjectMapper json = new ObjectMapper();
    private final RequestHasher hasher = new RequestHasher(json);

    private String hash(String text) throws Exception {
        return hasher.hash(json.readTree(text));
    }

    @Test
    @DisplayName("T09.extra-2 the hash ignores object key order at every depth but not array order or values")
    void canonicalForm() throws Exception {
        String base = hash("{\"a\":1,\"b\":{\"x\":[1,{\"p\":1,\"q\":2}],\"y\":null}}");

        assertThat(hash("{\"b\":{\"y\":null,\"x\":[1,{\"q\":2,\"p\":1}]},\"a\":1}")).isEqualTo(base);
        assertThat(hash("{\"a\":1,\"b\":{\"x\":[{\"p\":1,\"q\":2},1],\"y\":null}}")).isNotEqualTo(base);
        assertThat(hash("{\"a\":2,\"b\":{\"x\":[1,{\"p\":1,\"q\":2}],\"y\":null}}")).isNotEqualTo(base);
        assertThat(base).hasSize(64).matches("[0-9a-f]+");
    }
}
