package org.example.pubsub;

import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.example.pubsub.auth.ApiKeyAuthFilter;
import org.example.pubsub.auth.ApiKeyAuthenticator;
import org.example.pubsub.core.TopicManager;
import org.example.pubsub.model.Subscriber;
import org.example.pubsub.rest.RestApiHandler;
import org.example.pubsub.websocket.PubSubWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PubSubApplication {
    private static final Logger logger = LoggerFactory.getLogger(PubSubApplication.class);

    private final Server server;
    private final TopicManager topicManager;
    private final ApiKeyAuthenticator authenticator;
    private final AtomicReference<PubSubWebSocketHandler> wsHandlerRef;
    private final CountDownLatch shutdownLatch;
    private final int port;
    private final int maxDeliveryPerCycle;

    public PubSubApplication(int port, int queueCapacity, Subscriber.BackpressurePolicy backpressurePolicy, 
                             ApiKeyAuthenticator authenticator, int maxDeliveryPerCycle) {
        this.port = port;
        this.topicManager = new TopicManager(queueCapacity, backpressurePolicy);
        this.authenticator = authenticator;
        this.maxDeliveryPerCycle = maxDeliveryPerCycle;
        this.server = new Server();
        this.wsHandlerRef = new AtomicReference<>();
        this.shutdownLatch = new CountDownLatch(1);

        configureServer();
        configureShutdownHook();
    }

    private void configureServer() {
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        connector.setIdleTimeout(Duration.ofHours(1).toMillis());
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        if (authenticator.isAuthEnabled()) {
            FilterHolder authFilter = new FilterHolder(new ApiKeyAuthFilter(authenticator.getValidApiKeys()));
            context.addFilter(authFilter, "/api/*", EnumSet.of(DispatcherType.REQUEST));
        }

        ServletHolder restHolder = new ServletHolder("rest", new RestApiHandler(topicManager));
        context.addServlet(restHolder, "/api/*");

        JettyWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
            container.setIdleTimeout(Duration.ofHours(1));
            container.setMaxTextMessageSize(65536);
            
            PubSubWebSocketHandler handler = new PubSubWebSocketHandler(topicManager, authenticator, maxDeliveryPerCycle);
            wsHandlerRef.set(handler);
            container.addMapping("/ws", (upgradeRequest, upgradeResponse) -> {
                if (authenticator.isAuthEnabled()) {
                    var apiKeyHeaders = upgradeRequest.getHeaders().get("X-API-Key");
                    String apiKey = (apiKeyHeaders != null && !apiKeyHeaders.isEmpty()) 
                        ? apiKeyHeaders.get(0) : null;
                    if (!authenticator.isValidApiKey(apiKey)) {
                        logger.warn("WebSocket connection rejected: invalid API key");
                        upgradeResponse.setStatusCode(401);
                        return null;
                    }
                }
                return handler;
            });
        });
    }

    private void configureShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            gracefulShutdown();
        }, "shutdown-hook"));
    }

    public void start() throws Exception {
        server.start();
        logger.info("PubSub server started on port {}", port);
        logger.info("WebSocket endpoint: ws://localhost:{}/ws", port);
        logger.info("REST API endpoint: http://localhost:{}/api", port);
    }

    public void awaitTermination() throws InterruptedException {
        shutdownLatch.await();
    }

    public void gracefulShutdown() {
        logger.info("Initiating graceful shutdown...");

        topicManager.initiateShutdown();

        PubSubWebSocketHandler handler = wsHandlerRef.get();
        if (handler != null) {
            handler.shutdown();
        }

        try {
            logger.info("Waiting for in-flight operations to complete...");
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            server.stop();
            logger.info("Server stopped");
        } catch (Exception e) {
            logger.error("Error stopping server", e);
        }

        shutdownLatch.countDown();
    }

    public TopicManager getTopicManager() {
        return topicManager;
    }

    public static void main(String[] args) {
        int port = getEnvInt("PORT", 8080);
        int queueCapacity = getEnvInt("QUEUE_CAPACITY", 100);
        int maxDeliveryPerCycle = getEnvInt("MAX_DELIVERY_PER_CYCLE", 0);
        String policyStr = System.getenv("BACKPRESSURE_POLICY");
        Subscriber.BackpressurePolicy policy = "DISCONNECT_SLOW_CONSUMER".equals(policyStr)
            ? Subscriber.BackpressurePolicy.DISCONNECT_SLOW_CONSUMER
            : Subscriber.BackpressurePolicy.DROP_OLDEST;
        
        String apiKeys = System.getenv("API_KEYS");
        ApiKeyAuthenticator authenticator = new ApiKeyAuthenticator(apiKeys);

        logger.info("Configuration: port={}, queueCapacity={}, backpressurePolicy={}, authEnabled={}, maxDeliveryPerCycle={}", 
            port, queueCapacity, policy, authenticator.isAuthEnabled(), maxDeliveryPerCycle);

        PubSubApplication app = new PubSubApplication(port, queueCapacity, policy, authenticator, maxDeliveryPerCycle);

        try {
            app.start();
            app.awaitTermination();
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            System.exit(1);
        }
    }

    private static int getEnvInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid value for {}: {}, using default: {}", name, value, defaultValue);
            }
        }
        return defaultValue;
    }
}
