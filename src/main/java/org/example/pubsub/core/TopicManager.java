package org.example.pubsub.core;

import org.example.pubsub.model.Message;
import org.example.pubsub.model.Subscriber;
import org.example.pubsub.model.Topic;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TopicManager {
    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);
    
    private final ConcurrentHashMap<String, Topic> topics;
    private final ConcurrentHashMap<Session, ConcurrentHashMap<String, Subscriber>> sessionSubscriptions;
    private final AtomicLong totalMessagesPublished;
    private final AtomicLong totalSubscriptions;
    private final AtomicBoolean shuttingDown;
    private final int defaultQueueCapacity;
    private final Subscriber.BackpressurePolicy defaultBackpressurePolicy;
    private final Instant startTime;

    public TopicManager() {
        this(100, Subscriber.BackpressurePolicy.DROP_OLDEST);
    }

    public TopicManager(int defaultQueueCapacity, Subscriber.BackpressurePolicy defaultBackpressurePolicy) {
        this.topics = new ConcurrentHashMap<>();
        this.sessionSubscriptions = new ConcurrentHashMap<>();
        this.totalMessagesPublished = new AtomicLong(0);
        this.totalSubscriptions = new AtomicLong(0);
        this.shuttingDown = new AtomicBoolean(false);
        this.defaultQueueCapacity = defaultQueueCapacity;
        this.defaultBackpressurePolicy = defaultBackpressurePolicy;
        this.startTime = Instant.now();
    }

    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    public void initiateShutdown() {
        shuttingDown.set(true);
        logger.info("TopicManager shutdown initiated");
    }

    public Topic createTopic(String name) {
        if (shuttingDown.get()) {
            throw new IllegalStateException("System is shutting down");
        }
        Topic topic = new Topic(name);
        Topic existing = topics.putIfAbsent(name, topic);
        if (existing != null) {
            return null;
        }
        logger.info("Created topic: {}", name);
        return topic;
    }

    public boolean deleteTopic(String name) {
        Topic removed = topics.remove(name);
        if (removed != null) {
            for (Subscriber sub : removed.getSubscribers()) {
                sessionSubscriptions.computeIfPresent(sub.getSession(), (session, subs) -> {
                    subs.remove(sub.getClientId() + ":" + name);
                    return subs.isEmpty() ? null : subs;
                });
            }
            logger.info("Deleted topic: {}", name);
            return true;
        }
        return false;
    }

    public Optional<Topic> getTopic(String name) {
        return Optional.ofNullable(topics.get(name));
    }

    public List<String> listTopics() {
        return List.copyOf(topics.keySet());
    }

    public List<Topic.TopicStats> getAllTopicStats() {
        return topics.values().stream()
            .map(Topic::getStats)
            .toList();
    }

    public SubscriptionResult subscribe(String topicName, String clientId, Session session, int lastN) {
        if (shuttingDown.get()) {
            return new SubscriptionResult(false, "System is shutting down", null);
        }

        Topic topic = topics.get(topicName);
        if (topic == null) {
            return new SubscriptionResult(false, "Topic not found: " + topicName, null);
        }

        Subscriber existingSubscriber = topic.getSubscriber(clientId);
        if (existingSubscriber != null) {
            return new SubscriptionResult(false, "Client already subscribed to topic", null);
        }

        Subscriber subscriber = new Subscriber(
            clientId,
            topicName,
            session,
            defaultQueueCapacity,
            defaultBackpressurePolicy
        );

        topic.addSubscriber(subscriber);
        
        sessionSubscriptions.computeIfAbsent(session, k -> new ConcurrentHashMap<>())
            .put(clientId + ":" + topicName, subscriber);
        
        totalSubscriptions.incrementAndGet();
        
        List<Message> history = lastN > 0 ? topic.getLastNMessages(lastN) : List.of();
        
        logger.info("Client {} subscribed to topic {} (replay {} messages)", clientId, topicName, history.size());
        return new SubscriptionResult(true, null, history);
    }

    public boolean unsubscribe(String topicName, String clientId, Session session) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            return false;
        }

        Subscriber removed = topic.removeSubscriber(clientId);
        if (removed != null) {
            sessionSubscriptions.computeIfPresent(session, (s, subs) -> {
                subs.remove(clientId + ":" + topicName);
                return subs.isEmpty() ? null : subs;
            });
            logger.info("Client {} unsubscribed from topic {}", clientId, topicName);
            return true;
        }
        return false;
    }

    public void removeAllSubscriptions(Session session) {
        ConcurrentHashMap<String, Subscriber> subscriptions = sessionSubscriptions.remove(session);
        if (subscriptions != null) {
            for (Subscriber sub : subscriptions.values()) {
                Topic topic = topics.get(sub.getTopicName());
                if (topic != null) {
                    topic.removeSubscriber(sub.getClientId());
                }
            }
            logger.info("Removed {} subscriptions for disconnected session", subscriptions.size());
        }
    }

    public PublishResult publish(String topicName, Message message) {
        if (shuttingDown.get()) {
            return new PublishResult(false, "System is shutting down", List.of());
        }

        Topic topic = topics.get(topicName);
        if (topic == null) {
            return new PublishResult(false, "Topic not found: " + topicName, List.of());
        }

        topic.addToHistory(message);
        totalMessagesPublished.incrementAndGet();

        List<Subscriber> subscribers = topic.getSubscribers();
        List<Subscriber> slowConsumers = subscribers.stream()
            .filter(sub -> !sub.enqueue(message))
            .toList();

        logger.debug("Published message {} to topic {} ({} subscribers, {} slow)", 
            message.id(), topicName, subscribers.size(), slowConsumers.size());
        
        return new PublishResult(true, null, slowConsumers);
    }

    public SystemStats getSystemStats() {
        int totalSubscribers = topics.values().stream()
            .mapToInt(Topic::getSubscriberCount)
            .sum();
        
        return new SystemStats(
            topics.size(),
            totalSubscribers,
            totalMessagesPublished.get(),
            totalSubscriptions.get(),
            sessionSubscriptions.size(),
            startTime,
            shuttingDown.get()
        );
    }

    public record SubscriptionResult(
        boolean success,
        String error,
        List<Message> historyMessages
    ) {}

    public record PublishResult(
        boolean success,
        String error,
        List<Subscriber> slowConsumers
    ) {}

    public record SystemStats(
        int topicCount,
        int totalSubscribers,
        long totalMessagesPublished,
        long totalSubscriptions,
        int activeConnections,
        Instant startTime,
        boolean shuttingDown
    ) {}
}
