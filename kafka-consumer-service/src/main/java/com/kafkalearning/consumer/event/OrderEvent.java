package com.kafkalearning.consumer.event;

import java.time.Instant;

/**
 * Domain event representing an order lifecycle action.
 * Kept as a simple record: immutable, no behavior, pure data transfer.
 */
public record OrderEvent(
        String orderId,
        String customerId,
        String status,
        double amount,
        Instant timestamp

) {
    public static OrderEvent created(String orderId, String customerId, double amount) {
        return new OrderEvent(orderId, customerId, "CREATED", amount, Instant.now());
    }
}
