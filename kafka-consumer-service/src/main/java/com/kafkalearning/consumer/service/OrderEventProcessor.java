package com.kafkalearning.consumer.service;

import com.kafkalearning.consumer.event.OrderEvent;

/**
 * Seam for actual business processing of a consumed OrderEvent.
 * Kept as an interface deliberately: the default implementation is a
 * placeholder (logging only), but this is also the seam tests use to
 * simulate transient failures for retry/backoff verification.
 */
public interface OrderEventProcessor {
    void process(OrderEvent orderEvent);
}
