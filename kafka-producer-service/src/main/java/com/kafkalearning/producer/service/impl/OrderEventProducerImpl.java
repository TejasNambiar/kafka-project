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