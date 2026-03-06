package org.example.pubsub.auth;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class ApiKeyAuthFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(ApiKeyAuthFilter.class);
    private static final String API_KEY_HEADER = "X-API-Key";

    private final Set<String> validApiKeys;
    private final Set<String> publicPaths;

    public ApiKeyAuthFilter(Set<String> validApiKeys) {
        this.validApiKeys = validApiKeys;
        this.publicPaths = Set.of("/api/health");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        String path = httpRequest.getRequestURI();
        
        if (publicPaths.contains(path)) {
            chain.doFilter(request, response);
            return;
        }

        String apiKey = httpRequest.getHeader(API_KEY_HEADER);

        if (apiKey == null || apiKey.isBlank()) {
            logger.warn("Missing API key for request: {} {}", httpRequest.getMethod(), path);
            sendUnauthorized(httpResponse, "Missing X-API-Key header");
            return;
        }

        if (!validApiKeys.contains(apiKey)) {
            logger.warn("Invalid API key for request: {} {}", httpRequest.getMethod(), path);
            sendUnauthorized(httpResponse, "Invalid API key");
            return;
        }

        chain.doFilter(request, response);
    }

    private void sendUnauthorized(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}
}
