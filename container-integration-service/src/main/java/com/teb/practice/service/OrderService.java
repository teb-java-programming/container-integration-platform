package com.teb.practice.service;

import com.teb.practice.event.OrderCreatedEvent;
import com.teb.practice.event.OrderEventPublisher;
import com.teb.practice.exception.OrderNotFoundException;
import com.teb.practice.messaging.OrderMessagePublisher;
import com.teb.practice.messaging.OrderProcessingMessage;
import com.teb.practice.model.Order;
import com.teb.practice.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderValidationClient validationClient;
    private final OrderEventPublisher eventPublisher;
    private final OrderMessagePublisher messagePublisher;

    public Order create(Order order) {

        validateRequest(order);

        if (!validationClient.validate(order.getCustomerId(), order.getProductId())) {
            throw new IllegalArgumentException("Order validation failed");
        }

        order.setStatus("CREATED");

        Order savedOrder = orderRepository.save(order);

        eventPublisher.publish(
                new OrderCreatedEvent(
                        savedOrder.getId(),
                        savedOrder.getCustomerId(),
                        savedOrder.getProductId(),
                        savedOrder.getQuantity()));

        messagePublisher.publish(
                new OrderProcessingMessage(
                        savedOrder.getId(),
                        savedOrder.getCustomerId(),
                        savedOrder.getProductId(),
                        savedOrder.getQuantity()));

        return savedOrder;
    }

    public Order findById(Long id) {

        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private void validateRequest(Order order) {

        if (order == null
                || order.getCustomerId() == null
                || order.getCustomerId().isBlank()
                || order.getProductId() == null
                || order.getProductId().isBlank()
                || order.getQuantity() == null
                || order.getQuantity() <= 0) {
            throw new IllegalArgumentException("Invalid order request");
        }
    }
}
