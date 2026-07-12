package com.kafkalearning.consumer.listener;

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

    @KafkaListener(topics = "${app.kafka.topic.orders-events}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void handleOrderEvent(ConsumerRecord<String, String> record) {
        // Taking the raw ConsumerRecord (rather than just the deserialized
        // value) so we can log partition/offset — visibility learners need
        // to build intuition for how records map to partitions/offsets.
        try {
            OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
            log.info("Consumed orderId={} status={} amount={} from partition={} offset={}",
                    event.orderId(), event.status(), event.amount(),
                    record.partition(), record.offset());

            // Phase 3 replaces this comment with real processing logic
            // and introduces error-handling/DLT for failures here.
        } catch (Exception e) {
            log.error("Failed to process record at partition={} offset={}: {}",
                    record.partition(), record.offset(), record.value(), e);

            // Intentionally NOT rethrown yet — Phase 3 introduces the
            // DefaultErrorHandler + retry/DLT strategy for this exact case.
            // For now, swallowing here means the offset still commits and
            // we move on — acceptable only because we're mid-phase; flagged
            // explicitly so it isn't mistaken for a finished design.
        }
    }
}