package com.kafkalearning.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.producer.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "orders-events-test")
public class OrderEventProducerEmbeddedKafkaTest {

    @Autowired
    private OrderEventProducer orderEventProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    // Point the real application config at the embedded broker's random
    // port instead of localhost:9092 — this is what lets @SpringBootTest
    // boot the actual KafkaProducerConfig beans against a real, ephemeral,
    // in-memory broker.
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry){
        registry.add("app.kafka.topic.orders-events", ()->"orders-events-test");
    }

    @Test
    void publish_actuallyDeliversMessageToEmbeddedBroker_withCorrectPayload() {
        OrderEvent event = OrderEvent.created("order-embedded-1", "cust-embedded", 99.95);

        orderEventProducer.publishOrderEvent(event);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-verification-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "orders-events-test");

            ConsumerRecord<String, String> received =
                    KafkaTestUtils.getSingleRecord(consumer, "orders-events-test", Duration.ofSeconds(10));

            assertThat(received.key()).isEqualTo("order-embedded-1");

            OrderEvent deserialized = readEvent(received.value());
            assertThat(deserialized.orderId()).isEqualTo("order-embedded-1");
            assertThat(deserialized.customerId()).isEqualTo("cust-embedded");
            assertThat(deserialized.status()).isEqualTo("CREATED");
            assertThat(deserialized.amount()).isEqualTo(99.95);
        }
    }

    private OrderEvent readEvent(String json) {
        try {
            return objectMapper.readValue(json, OrderEvent.class);
        } catch (Exception e) {
            throw new AssertionError("Failed to deserialize test record", e);
        }
    }
}
