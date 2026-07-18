package com.kafkalearning.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalearning.consumer.service.OrderEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.consumer.event.OrderEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumes {@code orders-events} and delegates to {@link OrderEventProcessor}.
 *
 * <p><b>Exception propagation is deliberate, not an oversight.</b> This
 * method does not catch {@link JsonProcessingException} or any exception
 * thrown by the processor — both propagate uncaught, up to the
 * {@code DefaultErrorHandler} configured in {@code KafkaConsumerConfig}.
 * That handler is what applies retry-with-backoff and dead-letter-topic
 * routing. An earlier version of this class (Phase 2) caught and logged
 * these exceptions inline, which silently discarded failed records with
 * no retry and no recovery path — see that phase's history for why this
 * was changed.
 *
 * <p>Takes the raw {@link ConsumerRecord} rather than an
 * already-deserialized value, specifically so partition/offset can be
 * logged alongside each record — useful for building intuition about how
 * records map to partitions, and it's also why deserialization failure
 * is something this class handles explicitly rather than failing
 * invisibly inside framework machinery.
 *
 * <p>{@code id} and {@code containerFactory} are both set explicitly
 * (rather than relying on Spring's naming conventions): {@code id} lets
 * tests look up this exact container via
 * {@code KafkaListenerEndpointRegistry} to wait for partition assignment
 * before publishing (fixes a real race condition hit in Phase 4);
 * {@code containerFactory} removes any ambiguity about which factory —
 * and therefore which error handler — this listener is bound to, after
 * a bug where the intended error handler bean existed but was never
 * actually attached to the container.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final ObjectMapper objectMapper;
    private final OrderEventProcessor processor;

    /**
     * No try/catch swallowing anymore. Deserialization failures
     * propagate up to DefaultErrorHandler, which classifies
     * JsonProcessingException as non-retryable and routes it
     * straight to the DLT (see KafkaConsumerConfig).
     */
    @KafkaListener(
            id = "orderEventListenerContainer",
            topics = "${app.kafka.topic.orders-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {

        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);

        log.info("Consumed orderId={} status={} amount={} from partition={} offset={}",
                event.orderId(), event.status(), event.amount(),
                record.partition(), record.offset());

        // Any exception thrown here is retryable by default — caught by
        // DefaultErrorHandler, retried 3x with backoff, then DLT-routed.
        processor.process(event);
    }
}