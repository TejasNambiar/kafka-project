package com.kafkalearning.consumer.service;


import com.kafkalearning.consumer.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Placeholder production implementation. Real business logic (updating
 * an order status table, triggering downstream workflows, etc.) is out
 * of scope for this learning project — this exists so the listener has
 * a genuine extension point rather than an inline no-op.
 */
@Slf4j
@Component
public class LoggingOrderEventProcessor implements OrderEventProcessor {

    @Override
    public void process(OrderEvent event) {
        log.info("Processing orderId={} (placeholder — no real business logic yet)",
                event.orderId());
    }
}
