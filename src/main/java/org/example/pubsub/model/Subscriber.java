package org.example.pubsub.model;

import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Subscriber {
    private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);
    
    private final String clientId;
    private final String topicName;
    private final Session session;
    private final BlockingQueue<Message> messageQueue;
    private final Instant subscribedAt;
    private final AtomicLong deliveredCount;
    private final AtomicLong droppedCount;
    private final AtomicBoolean slowConsumer;
    private final int queueCapacity;
    private final BackpressurePolicy backpressurePolicy;

    public enum BackpressurePolicy {
        DROP_OLDEST,
        DISCONNECT_SLOW_CONSUMER
    }

    public Subscriber(String clientId, String topicName, Session session, int queueCapacity, BackpressurePolicy policy) {
        this.clientId = clientId;
        this.topicName = topicName;
        this.session = session;
        this.queueCapacity = queueCapacity;
        this.messageQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.subscribedAt = Instant.now();
        this.deliveredCount = new AtomicLong(0);
        this.droppedCount = new AtomicLong(0);
        this.slowConsumer = new AtomicBoolean(false);
        this.backpressurePolicy = policy;
    }

    public String getClientId() {
        return clientId;
    }

    public String getTopicName() {
        return topicName;
    }

    public Session getSession() {
        return session;
    }

    public Instant getSubscribedAt() {
        return subscribedAt;
    }

    public long getDeliveredCount() {
        return deliveredCount.get();
    }

    public long getDroppedCount() {
        return droppedCount.get();
    }

    public boolean isSlowConsumer() {
        return slowConsumer.get();
    }

    public int getQueueSize() {
        return messageQueue.size();
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public BackpressurePolicy getBackpressurePolicy() {
        return backpressurePolicy;
    }

    public boolean enqueue(Message message) {
        boolean offered = messageQueue.offer(message);
        if (!offered) {
            if (backpressurePolicy == BackpressurePolicy.DROP_OLDEST) {
                messageQueue.poll();
                messageQueue.offer(message);
                droppedCount.incrementAndGet();
                logger.warn("Dropped oldest message for slow consumer {} on topic {}", clientId, topicName);
                return true;
            } else {
                slowConsumer.set(true);
                droppedCount.incrementAndGet();
                logger.warn("Slow consumer detected: {} on topic {}", clientId, topicName);
                return false;
            }
        }
        return true;
    }

    public Message poll() {
        Message msg = messageQueue.poll();
        if (msg != null) {
            deliveredCount.incrementAndGet();
        }
        return msg;
    }

    public void markDelivered() {
        deliveredCount.incrementAndGet();
    }

    public SubscriberStats getStats() {
        return new SubscriberStats(
            clientId,
            topicName,
            deliveredCount.get(),
            droppedCount.get(),
            messageQueue.size(),
            queueCapacity,
            slowConsumer.get(),
            subscribedAt
        );
    }

    public record SubscriberStats(
        String clientId,
        String topicName,
        long deliveredCount,
        long droppedCount,
        int queueSize,
        int queueCapacity,
        boolean slowConsumer,
        Instant subscribedAt
    ) {}
}
