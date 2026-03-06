package org.example.pubsub.model;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {
    private final String name;
    private final Instant createdAt;
    private final ConcurrentHashMap<String, Subscriber> subscribers;
    private final CopyOnWriteArrayList<Message> messageHistory;
    private final AtomicLong publishedCount;
    private final int maxHistorySize;

    public Topic(String name) {
        this(name, 1000);
    }

    public Topic(String name, int maxHistorySize) {
        this.name = name;
        this.createdAt = Instant.now();
        this.subscribers = new ConcurrentHashMap<>();
        this.messageHistory = new CopyOnWriteArrayList<>();
        this.publishedCount = new AtomicLong(0);
        this.maxHistorySize = maxHistorySize;
    }

    public String getName() {
        return name;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void addSubscriber(Subscriber subscriber) {
        subscribers.put(subscriber.getClientId(), subscriber);
    }

    public Subscriber removeSubscriber(String clientId) {
        return subscribers.remove(clientId);
    }

    public Subscriber getSubscriber(String clientId) {
        return subscribers.get(clientId);
    }

    public List<Subscriber> getSubscribers() {
        return List.copyOf(subscribers.values());
    }

    public int getSubscriberCount() {
        return subscribers.size();
    }

    public void addToHistory(Message message) {
        messageHistory.add(message);
        publishedCount.incrementAndGet();
        
        while (messageHistory.size() > maxHistorySize) {
            messageHistory.remove(0);
        }
    }

    public List<Message> getLastNMessages(int n) {
        int size = messageHistory.size();
        if (n <= 0 || size == 0) {
            return List.of();
        }
        int start = Math.max(0, size - n);
        return List.copyOf(messageHistory.subList(start, size));
    }

    public long getPublishedCount() {
        return publishedCount.get();
    }

    public TopicStats getStats() {
        return new TopicStats(
            name,
            subscribers.size(),
            publishedCount.get(),
            messageHistory.size(),
            createdAt
        );
    }

    public record TopicStats(
        String name,
        int subscriberCount,
        long publishedCount,
        int historySize,
        Instant createdAt
    ) {}
}
