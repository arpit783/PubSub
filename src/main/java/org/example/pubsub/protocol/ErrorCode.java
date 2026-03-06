package org.example.pubsub.protocol;

public enum ErrorCode {
    BAD_REQUEST,
    TOPIC_NOT_FOUND,
    TOPIC_ALREADY_EXISTS,
    INVALID_MESSAGE,
    SLOW_CONSUMER,
    INTERNAL_ERROR,
    SHUTTING_DOWN
}
