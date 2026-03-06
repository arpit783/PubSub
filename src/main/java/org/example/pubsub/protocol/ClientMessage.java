package org.example.pubsub.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.example.pubsub.model.Message;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ClientMessage(
    @JsonProperty("type") MessageType type,
    @JsonProperty("topic") String topic,
    @JsonProperty("message") MessagePayload message,
    @JsonProperty("client_id") String clientId,
    @JsonProperty("last_n") Integer lastN,
    @JsonProperty("request_id") String requestId
) {
    public enum MessageType {
        subscribe,
        unsubscribe,
        publish,
        ping
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MessagePayload(
        @JsonProperty("id") String id,
        @JsonProperty("payload") String payload
    ) {
        public Message toMessage() {
            return new Message(id, payload);
        }
    }
}
