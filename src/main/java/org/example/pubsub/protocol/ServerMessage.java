package org.example.pubsub.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.example.pubsub.model.Message;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ServerMessage(
    @JsonProperty("type") MessageType type,
    @JsonProperty("request_id") String requestId,
    @JsonProperty("topic") String topic,
    @JsonProperty("message") MessagePayload message,
    @JsonProperty("error") ErrorPayload error,
    @JsonProperty("ts") Instant ts
) {
    public enum MessageType {
        ack,
        event,
        error,
        pong,
        info
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record MessagePayload(
        @JsonProperty("id") String id,
        @JsonProperty("payload") String payload
    ) {
        public static MessagePayload from(Message message) {
            return new MessagePayload(message.id(), message.payload());
        }
    }

    public record ErrorPayload(
        @JsonProperty("code") String code,
        @JsonProperty("message") String message
    ) {}

    public static ServerMessage ack(String requestId, String topic) {
        return new ServerMessage(MessageType.ack, requestId, topic, null, null, Instant.now());
    }

    public static ServerMessage event(String topic, Message message) {
        return new ServerMessage(
            MessageType.event,
            null,
            topic,
            MessagePayload.from(message),
            null,
            Instant.now()
        );
    }

    public static ServerMessage pong(String requestId) {
        return new ServerMessage(MessageType.pong, requestId, null, null, null, Instant.now());
    }

    public static ServerMessage error(String requestId, ErrorCode code, String message) {
        return new ServerMessage(
            MessageType.error,
            requestId,
            null,
            null,
            new ErrorPayload(code.name(), message),
            Instant.now()
        );
    }

    public static ServerMessage info(String requestId, String topic, String infoMessage) {
        return new ServerMessage(
            MessageType.info,
            requestId,
            topic,
            new MessagePayload(null, infoMessage),
            null,
            Instant.now()
        );
    }
}
