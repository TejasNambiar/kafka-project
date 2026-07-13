package com.kafkalearning.producer.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.producer.event.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderEventProducerImplTest {

    private static final String TOPIC = "orders-events";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    private OrderEventProducerImpl producer;

    @BeforeEach
    void setup(){
        objectMapper = new ObjectMapper().findAndRegisterModules(); // registers JavaTimeModule for Instant
        producer = new OrderEventProducerImpl(kafkaTemplate, objectMapper, TOPIC);
    }

    @Test
    void publish_sendsRecordWithOrderIdAsKey_andCorrectTopic(){
        OrderEvent event = new OrderEvent("order-123", "cust-1", "CREATED", 49.99, Instant.now());

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0), 0, 0,
                0, 0, 0
        );
        SendResult<String, String> sendResult = new SendResult<>(
                new ProducerRecord<>(TOPIC, event.orderId(), "ignored"), metadata);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        producer.publishOrderEvent(event);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(1)).send(captor.capture());

        ProducerRecord<String, String> sentRecord = captor.getValue();
        assertThat(sentRecord.topic()).isEqualTo(TOPIC);
        assertThat(sentRecord.key()).isEqualTo("order-123"); // partition key = orderId, per ADR
        assertThat(sentRecord.value()).contains("\"orderId\":\"order-123\"");
        assertThat(sentRecord.value()).contains("\"status\":\"CREATED\"");
    }

    @Test
    void publish_whenSendFailsAsynchronously_doesNotThrow_justLogsFailure(){
        OrderEvent event = new OrderEvent("order-456", "cust-2", "CREATED", 10.0, Instant.now());

        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("simulated broker unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        // The whole point of Phase 2's async design: a downstream send
        // failure must NOT propagate as an exception out of publish() —
        // it's handled entirely inside the whenComplete callback.
        assertDoesNotThrow(() -> producer.publishOrderEvent(event));
    }

    @Test
    void publish_whenSerializationFails_throwsImmediately_andNeverCallsKafkaTemplate() {
        // A poison-pill scenario: force serialization to fail by using an
        // ObjectMapper that can't handle a type it's never seen configured.
        ObjectMapper brokenMapper = mock(ObjectMapper.class);
        try {
            when(brokenMapper.writeValueAsString(any()))
                    .thenThrow(new JsonProcessingException("forced failure") {});
        } catch (JsonProcessingException e) {
            throw new AssertionError("unreachable — mock setup only", e);
        }

        OrderEventProducerImpl producerWithBrokenMapper =
                new OrderEventProducerImpl(kafkaTemplate, brokenMapper, TOPIC);

        OrderEvent event = new OrderEvent("order-789", "cust-3", "CREATED", 5.0, Instant.now());

        assertThatThrownBy(() -> producerWithBrokenMapper.publishOrderEvent(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Could not serialize OrderEvent");

        // Fail-fast means we never even attempt the send.
        verifyNoInteractions(kafkaTemplate);
    }

}