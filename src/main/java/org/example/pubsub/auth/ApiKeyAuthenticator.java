package org.example.pubsub.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class ApiKeyAuthenticator {
    private static final Logger logger = LoggerFactory.getLogger(ApiKeyAuthenticator.class);
    
    private final Set<String> validApiKeys;
    private final boolean authEnabled;

    public ApiKeyAuthenticator(String apiKeysConfig) {
        if (apiKeysConfig == null || apiKeysConfig.isBlank()) {
            this.validApiKeys = Collections.emptySet();
            this.authEnabled = false;
            logger.warn("API key authentication is DISABLED. Set API_KEYS environment variable to enable.");
        } else {
            this.validApiKeys = Arrays.stream(apiKeysConfig.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
            this.authEnabled = !validApiKeys.isEmpty();
            logger.info("API key authentication enabled with {} key(s)", validApiKeys.size());
        }
    }

    public boolean isAuthEnabled() {
        return authEnabled;
    }

    public boolean isValidApiKey(String apiKey) {
        if (!authEnabled) {
            return true;
        }
        return apiKey != null && validApiKeys.contains(apiKey);
    }

    public Set<String> getValidApiKeys() {
        return Collections.unmodifiableSet(validApiKeys);
    }
}
