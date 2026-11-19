package com.teb.practice.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@SpringBootTest
@AutoConfigureMockMvc
class OrderIntegrationTest {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final JsonMapper jsonMapper = new JsonMapper();
    @Autowired private MockMvc mockMvc;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private JmsTemplate jmsTemplate;

    @BeforeEach
    void stubValidationService() throws Exception {
        jdbcTemplate.update("DELETE FROM ORDERS");

        HttpRequest resetRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8081/__admin/reset"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();

        HttpResponse<String> resetResponse =
                httpClient.send(resetRequest, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, resetResponse.statusCode());

        String mapping =
                """
                {
                  "request": {
                    "method": "GET",
                    "urlPath": "/validation/customers/CUST-001/products/PROD-001"
                  },
                  "response": {
                    "status": 200,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {
                      "valid": true
                    }
                  }
                }
                """;

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8081/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapping))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(201, response.statusCode());
    }

    @Test
    void shouldCreateAndPublishOrderCreatedEvent() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG, "order-integration-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("orders"));

            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }

            String request =
                    """
                    {
                      "customerId": "CUST-001",
                      "productId": "PROD-001",
                      "quantity": 2
                    }
                    """;

            MvcResult result =
                    mockMvc.perform(
                                    post("/api/orders")
                                            .contentType("application/json")
                                            .content(request))
                            .andExpect(status().isCreated())
                            .andReturn();

            JsonNode responseBody = jsonMapper.readTree(result.getResponse().getContentAsString());

            long orderId = responseBody.get("id").asLong();

            ConsumerRecord<String, String> matchingRecord = null;
            long deadline = System.currentTimeMillis() + 10_000;

            while (System.currentTimeMillis() < deadline && matchingRecord == null) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(500))) {

                    JsonNode event = jsonMapper.readTree(record.value());

                    if (event.get("orderId").asLong() == orderId) {
                        matchingRecord = record;
                        break;
                    }
                }
            }

            assertNotNull(matchingRecord, "OrderCreated event was not published");

            JsonNode event = jsonMapper.readTree(matchingRecord.value());

            assertEquals(orderId, event.get("orderId").asLong());
            assertEquals("CUST-001", event.get("customerId").asString());
            assertEquals("PROD-001", event.get("productId").asString());
            assertEquals(2, event.get("quantity").asInt());

            jmsTemplate.setReceiveTimeout(1000);

            String mqMessage = null;
            long mqDeadline = System.currentTimeMillis() + 10_000;

            while (System.currentTimeMillis() < mqDeadline && mqMessage == null) {
                Object message = jmsTemplate.receiveAndConvert("DEV.QUEUE.1");

                if (message != null) {
                    String candidate = message.toString();
                    JsonNode mqJson = jsonMapper.readTree(candidate);

                    if (mqJson.get("orderId").asLong() == orderId) {
                        mqMessage = candidate;
                    }
                }
            }

            assertNotNull(mqMessage, "OrderProcessing message was not published");

            JsonNode mqJson = jsonMapper.readTree(mqMessage);

            assertEquals(orderId, mqJson.get("orderId").asLong());
            assertEquals("CUST-001", mqJson.get("customerId").asString());
            assertEquals("PROD-001", mqJson.get("productId").asString());
            assertEquals(2, mqJson.get("quantity").asInt());
        }
    }

    @Test
    void shouldRejectInvalidOrder() throws Exception {
        String request =
                """
                {
                  "customerId": "",
                  "productId": "",
                  "quantity": 0
                }
                """;

        mockMvc.perform(post("/api/orders").contentType("application/json").content(request))
                .andExpect(status().isBadRequest());

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ORDERS", Integer.class);

        assertEquals(0, count);
    }

    @Test
    void shouldRejectOrderWhenExternalValidationFails() throws Exception {
        String mapping =
                """
                {
                  "request": {
                    "method": "GET",
                    "urlPath": "/validation/customers/CUST-001/products/PROD-001"
                  },
                  "response": {
                    "status": 200,
                    "headers": {
                      "Content-Type": "application/json"
                    },
                    "jsonBody": {
                      "valid": false
                    }
                  }
                }
                """;

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8081/__admin/mappings"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapping))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(201, response.statusCode());

        String orderRequest =
                """
                {
                  "customerId": "CUST-001",
                  "productId": "PROD-001",
                  "quantity": 2
                }
                """;

        mockMvc.perform(post("/api/orders").contentType("application/json").content(orderRequest))
                .andExpect(status().isBadRequest());

        Integer count =
                jdbcTemplate.queryForObject(
                        """
                        SELECT COUNT(*)
                        FROM ORDERS
                        WHERE CUSTOMER_ID = ?
                          AND PRODUCT_ID = ?
                          AND QUANTITY = ?
                        """,
                        Integer.class,
                        "CUST-001",
                        "PROD-001",
                        2);

        assertEquals(0, count);
    }

    @Test
    void shouldRetrieveExistingOrder() throws Exception {
        String request =
                """
                {
                  "customerId": "CUST-001",
                  "productId": "PROD-001",
                  "quantity": 2
                }
                """;

        MvcResult createResult =
                mockMvc.perform(
                                post("/api/orders")
                                        .contentType("application/json")
                                        .content(request))
                        .andExpect(status().isCreated())
                        .andReturn();

        JsonNode createdOrder =
                jsonMapper.readTree(createResult.getResponse().getContentAsString());

        long orderId = createdOrder.get("id").asLong();

        MvcResult getResult =
                mockMvc.perform(get("/api/orders/{id}", orderId))
                        .andExpect(status().isOk())
                        .andReturn();

        JsonNode retrievedOrder = jsonMapper.readTree(getResult.getResponse().getContentAsString());

        assertEquals(orderId, retrievedOrder.get("id").asLong());
        assertEquals("CUST-001", retrievedOrder.get("customerId").asString());
        assertEquals("PROD-001", retrievedOrder.get("productId").asString());
        assertEquals(2, retrievedOrder.get("quantity").asInt());
        assertEquals("CREATED", retrievedOrder.get("status").asString());
    }

    @Test
    void shouldReturnNotFoundForMissingOrder() throws Exception {
        mockMvc.perform(get("/api/orders/{id}", 999999L)).andExpect(status().isNotFound());
    }
}
