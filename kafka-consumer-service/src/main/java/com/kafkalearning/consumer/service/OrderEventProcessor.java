package com.kafkalearning.consumer.service;

import com.kafkalearning.consumer.event.OrderEvent;

/**
 * Extension point for real business processing of a consumed
 * {@link OrderEvent} — e.g. updating an order-status table, triggering
 * a downstream workflow, etc. None of that exists yet; see
 * {@link LoggingOrderEventProcessor}.
 *
 * <p>This interface exists specifically as a testing seam (Phase 3),
 * not as speculative future-proofing. Before this existed, the
 * listener's business-processing step was inline and had no failure
 * point other than deserialization — there was no way to simulate a
 * transient downstream failure to verify retry/backoff behavior without
 * reflection hacks. Injecting this interface let tests substitute a
 * stub that fails on command (see
 * {@code OrderEventListenerRetryThenDltTest}), cleanly, from outside
 * production code.
 *
 * <p>Any exception thrown from {@link #process(OrderEvent)} propagates
 * up through {@code OrderEventListener} to {@code DefaultErrorHandler},
 * where it is retried with exponential backoff before being routed to
 * the dead letter topic if it never succeeds.
 */
public interface OrderEventProcessor {
    /**
     * Processes a single consumed order event.
     *
     * @param orderEvent the deserialized event to process
     */
    void process(OrderEvent orderEvent);
}
