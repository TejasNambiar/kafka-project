package com.kafkalearning.producer.controller;

import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkalearning.producer.event.OrderEvent;
import com.kafkalearning.producer.service.OrderEventProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REST entry point for creating orders. Translates an HTTP request into
 * an {@link OrderEvent} and hands it to {@link OrderEventProducer} —
 * contains no Kafka-specific logic itself.
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer orderEventProducer;


    /**
     * Inbound HTTP request shape. Kept nested and separate from
     * {@link OrderEvent} deliberately — this describes the HTTP contract,
     * not the domain event; the two are allowed to diverge independently.
     */
    public record CreateOrderRequest(String customerId, double amount) {
    }

    /**
     * Creates a new order and publishes it to {@code orders-events}.
     *
     * <p>Returns {@code 202 Accepted}, not {@code 201 Created} —
     * intentional. The response is sent as soon as publishing is
     * <i>initiated</i>, not once the record is durably committed on the
     * broker; {@code 202} honestly reflects that async contract rather
     * than implying stronger delivery guarantees than actually exist at
     * the point of response.
     *
     * @param request customerId and amount for the new order
     * @return the generated {@link OrderEvent}, including its newly
     *         minted {@code orderId}
     */
    @PostMapping("/createOrder")
    public ResponseEntity<OrderEvent> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("calling createOrder: " + request);
        OrderEvent event = OrderEvent.created(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.amount());
        orderEventProducer.publishOrderEvent(event);
        return ResponseEntity.accepted().body(event);
    }
}