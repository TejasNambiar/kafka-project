package com.kafkalearning.producer.event;

import java.time.Instant;

/**
 * Domain event representing an order lifecycle action, published to the
 * {@code orders-events} Kafka topic.
 *
 * <p>Modeled as an immutable {@code record} deliberately: once an event
 * is created and handed to the producer, nothing should mutate it —
 * doing so would risk a mismatch between what's held in memory and what
 * was actually serialized and sent to the broker.
 *
 * <p>{@link #timestamp} is an {@link Instant} (UTC, timezone-independent)
 * rather than {@code LocalDateTime}, since this event crosses process and
 * potentially machine/timezone boundaries between the producer and
 * consumer services.
 *
 * <p><b>Note:</b> this record is intentionally duplicated in
 * {@code kafka-consumer-service} rather than extracted into a shared
 * module (see ADR-001). If this shape changes, the consumer-side copy
 * must be updated in lockstep — there is currently no compiler-enforced
 * contract between the two.
 *
 * @param orderId    unique identifier for the order; also used as the
 *                   Kafka partition key to preserve per-order ordering
 * @param customerId identifier of the customer who placed the order
 * @param status     lifecycle status of the order (e.g. {@code CREATED})
 * @param amount     order total
 * @param timestamp  UTC instant the event was created
 */
public record OrderEvent(
        String orderId,
        String customerId,
        String status,
        double amount,
        Instant timestamp

) {
    /**
     * Creates a new {@code OrderEvent} representing an order just placed.
     * Centralizes the business rule that a freshly created order always
     * has status {@code CREATED} and a timestamp of "now" — callers don't
     * construct that state by hand.
     *
     * @param orderId    unique identifier for the new order
     * @param customerId identifier of the customer placing the order
     * @param amount     order total
     * @return a new {@code OrderEvent} with status {@code CREATED} and the
     *         current UTC instant
     */
    public static OrderEvent created(String orderId, String customerId, double amount) {
        return new OrderEvent(orderId, customerId, "CREATED", amount, Instant.now());
    }
}
