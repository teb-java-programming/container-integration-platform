package com.teb.practice.messaging;

public record OrderProcessingMessage(
        Long orderId, String customerId, String productId, Integer quantity) {}
