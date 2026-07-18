package com.kafkalearning.producer.service.impl;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.producer.event.OrderEvent;
import com.kafkalearning.producer.service.OrderEventProducer;

import lombok.extern.slf4j.Slf4j;

/**
 * Publishes {@link OrderEvent}s to the {@code orders-events} topic.
 *
 * <p><b>Partition key:</b> every record is keyed on {@link OrderEvent#orderId()},
 * never {@code customerId} or left unkeyed. Kafka only guarantees ordering
 * <i>within</i> a partition — keying on orderId guarantees every event for
 * a given order lands on the same partition in send order, which matters
 * once this service publishes more than just "CREATED" events (e.g. a
 * CANCELLED event must never be visible before its CREATED event).
 *
 * <p><b>Serialization:</b> uses plain {@code String} (de)serialization
 * with explicit Jackson conversion here, rather than Spring's
 * {@code JsonSerializer}. See ADR-002 — the trade-off is losing automatic
 * type-header resolution in exchange for a plain, human-readable JSON
 * wire format that's directly inspectable in kafka-ui.
 *
 * <p><b>Failure handling — two distinct failure classes, handled
 * differently on purpose:</b>
 * <ul>
 *   <li><b>Serialization failure</b> (this code can't convert the event
 *       to JSON) is treated as a programming error: unrecoverable by
 *       retry, so it fails fast with an {@link IllegalStateException}
 *       before any network call is attempted.</li>
 *   <li><b>Send failure</b> (broker unreachable, timeout) is a transient,
 *       external condition. {@code KafkaTemplate.send()} is asynchronous
 *       and does not block the caller; the {@code whenComplete} callback
 *       below is what surfaces a failed send at all — without it, a send
 *       failure would be entirely silent (no exception anywhere, the
 *       record just never arrives).</li>
 * </ul>
 */
@Slf4j
@Service
public class OrderEventProducerImpl implements OrderEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String ordersTopic;

    public OrderEventProducerImpl(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topic.orders-events}") String ordersTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.ordersTopic = ordersTopic;
    }

    /**
     * Serializes and publishes the given event, keyed on
     * {@link OrderEvent#orderId()}.
     *
     * <p>Returns immediately after initiating the send — this method does
     * not wait for broker acknowledgment. Success or failure of the
     * actual send is logged asynchronously via the callback attached to
     * the returned future; callers relying on delivery confirmation
     * beyond "the call didn't throw" would need a different contract
     * than this method currently offers.
     *
     * @param event the order event to publish
     * @throws IllegalStateException if the event cannot be serialized to
     *         JSON — treated as a non-retryable programming error, thrown
     *         before any send is attempted
     */
    @Override
    public void publishOrderEvent(OrderEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // Serialization failure is a programming error (bad record shape),
            // not a transient/retryable Kafka issue — fail fast and loud.
            log.error("Failed to serialize OrderEvent for orderId={}", event.orderId(), e);
            throw new IllegalStateException("Could not serialize OrderEvent", e);
        }

        // Partition key = orderId: guarantees all events for the same order
        // land in the same partition, preserving per-order ordering.
        ProducerRecord<String, String> record =
                new ProducerRecord<>(ordersTopic, event.orderId(), payload);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Published orderId={} to topic={} partition={} offset={}",
                        event.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                // Phase 3 introduces retry/idempotence config; for now we
                // just make the failure visible rather than losing it silently.
                log.error("Failed to publish orderId={} to topic={}",
                        event.orderId(), ordersTopic, ex);
            }
        });
    }
}