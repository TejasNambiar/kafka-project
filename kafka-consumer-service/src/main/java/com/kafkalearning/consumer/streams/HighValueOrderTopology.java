package com.kafkalearning.consumer.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.consumer.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Stateless topology: orders-events -> filter(amount > threshold) ->
 * high-value-orders. Deliberately narrow scope (ADR-006) — no
 * aggregation, no state store, no windowing. Those arrive in Phase 5.
 */
@Slf4j
@Configuration
public class HighValueOrderTopology {

    private static final double HIGH_VALUE_THRESHOLD = 1000.0;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Bean
    public KStream<String, String> highValueOrderStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.kafka.topic.orders-events}") String sourceTopic,
            @Value("${app.kafka.topic.high-value-orders}") String sinkTopic
    ){
        KStream<String, String> source = streamsBuilder.stream(sourceTopic);

        KStream<String, String> highValueOnly = source.filter((key, value) -> {
            try {
                OrderEvent event = objectMapper.readValue(value, OrderEvent.class);
                boolean isHighValue = event.amount() > HIGH_VALUE_THRESHOLD;
                if (isHighValue) {
                    log.info("Routing high-value order to {}: orderId={} amount={}",
                            sinkTopic, event.orderId(), event.amount());
                }
                return isHighValue;
            } catch (Exception e) {
                // Streams' filter predicate has no checked-exception escape
                // hatch — a malformed record here is silently excluded
                // (not routed anywhere). This is a real gap: Streams'
                // deserialization.exception.handler is the proper fix,
                // out of scope for this phase per ADR-006. Logged loudly
                // so it isn't a silent, undiscoverable gap.
                log.error("Skipping unparseable record in high-value-order topology, key={}", key, e);
                return false;
            }
        });

        highValueOnly.to(sinkTopic);

        return highValueOnly;
    }
}
