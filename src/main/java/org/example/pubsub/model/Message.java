package org.example.pubsub.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

public record Message(
    @JsonProperty("id") String id,
    @JsonProperty("payload") String payload,
    @JsonProperty("timestamp") Instant timestamp
) {
    public Message {
        Objects.requireNonNull(id, "Message id cannot be null");
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }

    public Message(String id, String payload) {
        this(id, payload, Instant.now());
    }
}
