# Build stage
FROM maven:3.9-eclipse-temurin-21-alpine AS build

WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests -B

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Copy the built jar
COPY --from=build /app/target/pubsub-1.0-SNAPSHOT.jar app.jar

# Change ownership
RUN chown -R appuser:appgroup /app

USER appuser

# Environment variables with defaults
ENV PORT=8080 \
    QUEUE_CAPACITY=100 \
    BACKPRESSURE_POLICY=DROP_OLDEST \
    API_KEYS="" \
    MAX_DELIVERY_PER_CYCLE=1 \
    JAVA_OPTS="-Xms256m -Xmx512m"

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:${PORT}/api/health || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
