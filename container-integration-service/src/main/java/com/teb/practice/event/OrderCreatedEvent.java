package com.teb.practice.event;

public record OrderCreatedEvent(
        Long orderId, String customerId, String productId, Integer quantity) {}
