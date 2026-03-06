package org.example.pubsub.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.example.pubsub.auth.ApiKeyAuthenticator;
import org.example.pubsub.core.TopicManager;
import org.example.pubsub.model.Message;
import org.example.pubsub.model.Subscriber;
import org.example.pubsub.protocol.ClientMessage;
import org.example.pubsub.protocol.ErrorCode;
import org.example.pubsub.protocol.ServerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@WebSocket
public class PubSubWebSocketHandler {
    private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketHandler.class);
    private static final long PING_INTERVAL_SECONDS = 30;

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<Session, SessionContext> sessions;
    private final ScheduledExecutorService deliveryExecutor;
    private final ScheduledExecutorService pingExecutor;
    private final int maxDeliveryPerCycle;

    public PubSubWebSocketHandler(TopicManager topicManager, ApiKeyAuthenticator authenticator) {
        this(topicManager, authenticator, 0);
    }

    public PubSubWebSocketHandler(TopicManager topicManager, ApiKeyAuthenticator authenticator, int maxDeliveryPerCycle) {
        this.topicManager = topicManager;
        this.maxDeliveryPerCycle = maxDeliveryPerCycle;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.sessions = new ConcurrentHashMap<>();
        this.deliveryExecutor = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r, "message-delivery");
                t.setDaemon(true);
                return t;
            }
        );
        this.pingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "websocket-ping");
            t.setDaemon(true);
            return t;
        });
        startDeliveryLoop();
        startPingLoop();
    }

    private void startDeliveryLoop() {
        deliveryExecutor.scheduleWithFixedDelay(this::deliverPendingMessages, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void startPingLoop() {
        pingExecutor.scheduleWithFixedDelay(this::sendPingsToAllSessions, 
            PING_INTERVAL_SECONDS, PING_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void sendPingsToAllSessions() {
        for (Session session : sessions.keySet()) {
            if (session.isOpen()) {
                try {
                    sendMessage(session, ServerMessage.pong("server-ping"));
                } catch (Exception e) {
                    logger.debug("Failed to send ping to session: {}", e.getMessage());
                }
            }
        }
    }

    private void deliverPendingMessages() {
        for (var entry : sessions.entrySet()) {
            Session session = entry.getKey();
            SessionContext ctx = entry.getValue();
            
            if (!session.isOpen()) {
                continue;
            }

            for (Subscriber subscriber : ctx.getSubscribers()) {
                if (subscriber.isSlowConsumer()) {
                    try {
                        ServerMessage error = ServerMessage.error(
                            null,
                            ErrorCode.SLOW_CONSUMER,
                            "Consumer too slow, disconnecting"
                        );
                        sendMessage(session, error);
                        session.close(1008, "SLOW_CONSUMER", Callback.NOOP);
                    } catch (Exception e) {
                        logger.error("Failed to close slow consumer session", e);
                    }
                    continue;
                }

                int delivered = 0;
                Message msg;
                while ((msg = subscriber.poll()) != null) {
                    try {
                        ServerMessage event = ServerMessage.event(subscriber.getTopicName(), msg);
                        sendMessage(session, event);
                        delivered++;
                        if (maxDeliveryPerCycle > 0 && delivered >= maxDeliveryPerCycle) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.error("Failed to deliver message to {}", subscriber.getClientId(), e);
                    }
                }
            }
        }
    }

    @OnWebSocketOpen
    public void onOpen(Session session) {
        logger.info("WebSocket connection opened: {}", session.getRemoteSocketAddress());
        sessions.put(session, new SessionContext());
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        logger.info("WebSocket connection closed: {} (code={}, reason={})", 
            session.getRemoteSocketAddress(), statusCode, reason);
        sessions.remove(session);
        topicManager.removeAllSubscriptions(session);
    }

    @OnWebSocketError
    public void onError(Session session, Throwable cause) {
        logger.error("WebSocket error for {}: {}", session.getRemoteSocketAddress(), cause.getMessage());
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String text) {
        try {
            ClientMessage clientMsg = objectMapper.readValue(text, ClientMessage.class);
            handleClientMessage(session, clientMsg);
        } catch (Exception e) {
            logger.error("Failed to parse message: {}", e.getMessage());
            sendError(session, null, ErrorCode.BAD_REQUEST, "Invalid message format: " + e.getMessage());
        }
    }

    private void handleClientMessage(Session session, ClientMessage msg) {
        if (topicManager.isShuttingDown() && msg.type() != ClientMessage.MessageType.ping) {
            sendError(session, msg.requestId(), ErrorCode.SHUTTING_DOWN, "System is shutting down");
            return;
        }

        switch (msg.type()) {
            case ping -> handlePing(session, msg);
            case subscribe -> handleSubscribe(session, msg);
            case unsubscribe -> handleUnsubscribe(session, msg);
            case publish -> handlePublish(session, msg);
        }
    }

    private void handlePing(Session session, ClientMessage msg) {
        sendMessage(session, ServerMessage.pong(msg.requestId()));
    }

    private void handleSubscribe(Session session, ClientMessage msg) {
        if (msg.topic() == null || msg.topic().isBlank()) {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, "Topic is required");
            return;
        }
        if (msg.clientId() == null || msg.clientId().isBlank()) {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, "client_id is required");
            return;
        }

        int lastN = msg.lastN() != null ? msg.lastN() : 0;
        TopicManager.SubscriptionResult result = topicManager.subscribe(
            msg.topic(), msg.clientId(), session, lastN
        );

        if (result.success()) {
            SessionContext ctx = sessions.get(session);
            Subscriber subscriber = null;
            if (ctx != null) {
                var topicOpt = topicManager.getTopic(msg.topic());
                if (topicOpt.isPresent()) {
                    subscriber = topicOpt.get().getSubscriber(msg.clientId());
                    if (subscriber != null) {
                        ctx.addSubscriber(subscriber);
                    }
                }
            }

            sendMessage(session, ServerMessage.ack(msg.requestId(), msg.topic()));

            for (Message historyMsg : result.historyMessages()) {
                sendMessage(session, ServerMessage.event(msg.topic(), historyMsg));
                if (subscriber != null) {
                    subscriber.markDelivered();
                }
            }
        } else {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, result.error());
        }
    }

    private void handleUnsubscribe(Session session, ClientMessage msg) {
        if (msg.topic() == null || msg.topic().isBlank()) {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, "Topic is required");
            return;
        }
        if (msg.clientId() == null || msg.clientId().isBlank()) {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, "client_id is required");
            return;
        }

        boolean success = topicManager.unsubscribe(msg.topic(), msg.clientId(), session);
        
        if (success) {
            SessionContext ctx = sessions.get(session);
            if (ctx != null) {
                ctx.removeSubscriber(msg.clientId(), msg.topic());
            }
            sendMessage(session, ServerMessage.ack(msg.requestId(), msg.topic()));
        } else {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, 
                "Not subscribed to topic: " + msg.topic());
        }
    }

    private void handlePublish(Session session, ClientMessage msg) {
        if (msg.topic() == null || msg.topic().isBlank()) {
            sendError(session, msg.requestId(), ErrorCode.BAD_REQUEST, "Topic is required");
            return;
        }
        if (msg.message() == null || msg.message().id() == null) {
            sendError(session, msg.requestId(), ErrorCode.INVALID_MESSAGE, "Message with id is required");
            return;
        }

        Message message = msg.message().toMessage();
        TopicManager.PublishResult result = topicManager.publish(msg.topic(), message);

        if (result.success()) {
            sendMessage(session, ServerMessage.ack(msg.requestId(), msg.topic()));
        } else {
            sendError(session, msg.requestId(), ErrorCode.TOPIC_NOT_FOUND, result.error());
        }
    }

    private void sendMessage(Session session, ServerMessage msg) {
        try {
            String json = objectMapper.writeValueAsString(msg);
            session.sendText(json, Callback.NOOP);
        } catch (Exception e) {
            logger.error("Failed to send message", e);
        }
    }

    private void sendError(Session session, String requestId, ErrorCode code, String message) {
        sendMessage(session, ServerMessage.error(requestId, code, message));
    }

    public void shutdown() {
        logger.info("Shutting down WebSocket handler");
        
        pingExecutor.shutdown();
        deliveryExecutor.shutdown();
        try {
            if (!pingExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                pingExecutor.shutdownNow();
            }
            if (!deliveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                deliveryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            pingExecutor.shutdownNow();
            deliveryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for (Session session : sessions.keySet()) {
            try {
                sendMessage(session, ServerMessage.info(null, null, "Server shutting down"));
                session.close(1001, "Server shutting down", Callback.NOOP);
            } catch (Exception e) {
                logger.error("Error closing session during shutdown", e);
            }
        }
    }

    private static class SessionContext {
        private final ConcurrentHashMap<String, Subscriber> subscribers = new ConcurrentHashMap<>();

        void addSubscriber(Subscriber subscriber) {
            subscribers.put(subscriber.getClientId() + ":" + subscriber.getTopicName(), subscriber);
        }

        void removeSubscriber(String clientId, String topicName) {
            subscribers.remove(clientId + ":" + topicName);
        }

        List<Subscriber> getSubscribers() {
            return List.copyOf(subscribers.values());
        }
    }
}
