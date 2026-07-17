package com.kafkalearning.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.consumer.event.OrderEvent;
import com.kafkalearning.consumer.service.OrderEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderEventListenerTest {

    private static final String TOPIC = "orders-events";

    @Mock
    private OrderEventProcessor processor;

    private ObjectMapper objectMapper;
    private OrderEventListener listener;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();
        listener = new OrderEventListener(objectMapper, processor);
    }

    @Test
    void handleOrderEvent_deserializesValidJson_andDelegatesToProcessor() throws Exception {
        OrderEvent event = new OrderEvent("order-1", "cust-1", "CREATED", 20.0, Instant.now());
        String json = objectMapper.writeValueAsString(event);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 5L, "order-1", json);

        listener.handleOrderEvent(record);

        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(processor).process(captor.capture());
        assertThat(captor.getValue().orderId()).isEqualTo("order-1");
        assertThat(captor.getValue().amount()).isEqualTo(20.0);
    }

    @Test
    void handleOrderEvent_onMalformedJson_throwsJsonProcessingException_andNeverCallsProcessor() {
        ConsumerRecord<String, String> malformedRecord =
                new ConsumerRecord<>(TOPIC, 0, 7L, "order-bad", "{not valid json");

        assertThatThrownBy(() -> listener.handleOrderEvent(malformedRecord))
                .isInstanceOf(JsonProcessingException.class);

        // Confirms the exception surfaces BEFORE processor delegation —
        // this is the exact behavior DefaultErrorHandler depends on to
        // classify it as non-retryable and route straight to DLT.
        verifyNoInteractions(processor);
    }

    @Test
    void handleOrderEvent_whenProcessorThrows_exceptionPropagatesUncaught() throws Exception {
        OrderEvent event = new OrderEvent("order-2", "cust-2", "CREATED", 5.0, Instant.now());
        String json = objectMapper.writeValueAsString(event);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 9L, "order-2", json);

        org.mockito.Mockito.doThrow(new RuntimeException("simulated transient downstream failure"))
                .when(processor).process(event);

        // Listener must NOT swallow this — propagation is what lets
        // DefaultErrorHandler apply retry/backoff.
        assertThatThrownBy(() -> listener.handleOrderEvent(record))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("simulated transient downstream failure");
    }
}