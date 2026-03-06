package org.example.pubsub.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.example.pubsub.core.TopicManager;
import org.example.pubsub.model.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RestApiHandler extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(RestApiHandler.class);
    
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^/topics/([^/]+)$");
    private static final Pattern TOPIC_STATS_PATTERN = Pattern.compile("^/topics/([^/]+)/stats$");
    
    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;

    public RestApiHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = req.getPathInfo();
        if (path == null) {
            path = "/";
        }

        try {
            switch (path) {
                case "/health" -> handleHealth(resp);
                case "/stats" -> handleStats(resp);
                case "/topics" -> handleListTopics(resp);
                default -> {
                    Matcher statsMatcher = TOPIC_STATS_PATTERN.matcher(path);
                    if (statsMatcher.matches()) {
                        handleTopicStats(statsMatcher.group(1), resp);
                        return;
                    }
                    
                    Matcher topicMatcher = TOPIC_PATTERN.matcher(path);
                    if (topicMatcher.matches()) {
                        handleGetTopic(topicMatcher.group(1), resp);
                    } else {
                        sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Not found");
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error handling GET request", e);
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = req.getPathInfo();
        if (path == null) {
            path = "/";
        }

        try {
            if (path.equals("/topics")) {
                handleCreateTopic(req, resp);
            } else {
                sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Not found");
            }
        } catch (Exception e) {
            logger.error("Error handling POST request", e);
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = req.getPathInfo();
        if (path == null) {
            sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Not found");
            return;
        }

        try {
            Matcher topicMatcher = TOPIC_PATTERN.matcher(path);
            if (topicMatcher.matches()) {
                handleDeleteTopic(topicMatcher.group(1), resp);
            } else {
                sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Not found");
            }
        } catch (Exception e) {
            logger.error("Error handling DELETE request", e);
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleHealth(HttpServletResponse resp) throws IOException {
        TopicManager.SystemStats stats = topicManager.getSystemStats();
        Map<String, Object> health = Map.of(
            "status", stats.shuttingDown() ? "shutting_down" : "healthy",
            "uptime_seconds", java.time.Duration.between(stats.startTime(), java.time.Instant.now()).getSeconds()
        );
        sendJson(resp, HttpServletResponse.SC_OK, health);
    }

    private void handleStats(HttpServletResponse resp) throws IOException {
        TopicManager.SystemStats stats = topicManager.getSystemStats();
        Map<String, Object> response = Map.of(
            "topics", stats.topicCount(),
            "total_subscribers", stats.totalSubscribers(),
            "total_messages_published", stats.totalMessagesPublished(),
            "total_subscriptions", stats.totalSubscriptions(),
            "active_connections", stats.activeConnections(),
            "start_time", stats.startTime().toString(),
            "shutting_down", stats.shuttingDown()
        );
        sendJson(resp, HttpServletResponse.SC_OK, response);
    }

    private void handleListTopics(HttpServletResponse resp) throws IOException {
        List<String> topics = topicManager.listTopics();
        sendJson(resp, HttpServletResponse.SC_OK, Map.of("topics", topics));
    }

    private void handleGetTopic(String topicName, HttpServletResponse resp) throws IOException {
        var topicOpt = topicManager.getTopic(topicName);
        if (topicOpt.isPresent()) {
            Topic.TopicStats stats = topicOpt.get().getStats();
            sendJson(resp, HttpServletResponse.SC_OK, stats);
        } else {
            sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Topic not found: " + topicName);
        }
    }

    private void handleTopicStats(String topicName, HttpServletResponse resp) throws IOException {
        var topicOpt = topicManager.getTopic(topicName);
        if (topicOpt.isPresent()) {
            Topic topic = topicOpt.get();
            Topic.TopicStats stats = topic.getStats();
            var subscriberStats = topic.getSubscribers().stream()
                .map(sub -> sub.getStats())
                .toList();
            
            Map<String, Object> response = Map.of(
                "topic", stats,
                "subscribers", subscriberStats
            );
            sendJson(resp, HttpServletResponse.SC_OK, response);
        } else {
            sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Topic not found: " + topicName);
        }
    }

    private void handleCreateTopic(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        CreateTopicRequest createReq = objectMapper.readValue(req.getInputStream(), CreateTopicRequest.class);
        
        if (createReq.name() == null || createReq.name().isBlank()) {
            sendError(resp, HttpServletResponse.SC_BAD_REQUEST, "Topic name is required");
            return;
        }

        Topic topic = topicManager.createTopic(createReq.name());
        if (topic != null) {
            sendJson(resp, HttpServletResponse.SC_CREATED, Map.of(
                "name", topic.getName(),
                "created_at", topic.getCreatedAt().toString()
            ));
        } else {
            sendError(resp, HttpServletResponse.SC_CONFLICT, "Topic already exists: " + createReq.name());
        }
    }

    private void handleDeleteTopic(String topicName, HttpServletResponse resp) throws IOException {
        boolean deleted = topicManager.deleteTopic(topicName);
        if (deleted) {
            resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
        } else {
            sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Topic not found: " + topicName);
        }
    }

    private void sendJson(HttpServletResponse resp, int status, Object data) throws IOException {
        resp.setStatus(status);
        resp.setContentType("application/json");
        objectMapper.writeValue(resp.getOutputStream(), data);
    }

    private void sendError(HttpServletResponse resp, int status, String message) throws IOException {
        resp.setStatus(status);
        resp.setContentType("application/json");
        objectMapper.writeValue(resp.getOutputStream(), Map.of("error", message));
    }

    private record CreateTopicRequest(String name) {}
}
