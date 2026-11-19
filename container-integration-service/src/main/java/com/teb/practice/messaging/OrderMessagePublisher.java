package com.teb.practice.messaging;

import lombok.RequiredArgsConstructor;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
@Component
public class OrderMessagePublisher {

    private static final String QUEUE_NAME = "DEV.QUEUE.1";

    private final JmsTemplate jmsTemplate;
    private final JsonMapper jsonMapper;

    public void publish(OrderProcessingMessage message) {

        try {
            jmsTemplate.convertAndSend(QUEUE_NAME, jsonMapper.writeValueAsString(message));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish order message", e);
        }
    }
}
