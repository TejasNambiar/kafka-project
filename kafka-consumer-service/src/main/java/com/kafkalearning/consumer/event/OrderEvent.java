package com.kafkalearning.consumer.event;

import java.time.Instant;

/**
 * Mirror of {@code com.kafkalearning.producer.event.OrderEvent}.
 *
 * <p>Deliberately duplicated rather than shared (see ADR-001) — at this
 * project's current scale, a shared module was judged not worth the
 * added build complexity. <b>This is the highest-risk drift point in the
 * codebase:</b> nothing at compile time enforces that this shape stays
 * in sync with the producer's copy. If the producer's {@code OrderEvent}
 * ever changes, this file must be updated by hand, in a separate module,
 * with no compiler error to catch a mismatch.
 *
 * <p>Unlike the producer's copy, this record has no factory method —
 * the consumer only ever deserializes events that already exist on the
 * wire; it has no business minting new ones, so a {@code created(...)}-style
 * factory here would misrepresent this module's role.
 *
 * @param orderId    unique identifier for the order; matches the Kafka
 *                   partition key used by the producer
 * @param customerId identifier of the customer who placed the order
 * @param status     lifecycle status of the order (e.g. {@code CREATED})
 * @param amount     order total
 * @param timestamp  UTC instant the event was originally created
 */
public record OrderEvent(
        String orderId,
        String customerId,
        String status,
        double amount,
        Instant timestamp

) {
}
