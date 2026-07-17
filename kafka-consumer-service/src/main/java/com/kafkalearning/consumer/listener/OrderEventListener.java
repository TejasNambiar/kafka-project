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
    @KafkaListener(topics = "${app.kafka.topic.orders-events}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
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