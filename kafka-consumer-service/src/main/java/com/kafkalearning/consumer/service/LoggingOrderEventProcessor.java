package com.kafkalearning.consumer.service;


import com.kafkalearning.consumer.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Placeholder production implementation of {@link OrderEventProcessor}.
 *
 * <p><b>This is intentionally unfinished.</b> No real business logic
 * exists yet — this class only logs. It exists to give
 * {@code OrderEventListener} a genuine implementation to delegate to
 * (rather than an inline no-op), and to mark exactly where real
 * processing logic (e.g. persisting order state, calling a downstream
 * service) should eventually be added.
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
