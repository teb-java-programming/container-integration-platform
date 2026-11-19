package com.teb.practice.service;

import com.teb.practice.model.OrderValidationResponse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class OrderValidationClient {

    private final RestClient restClient;

    public OrderValidationClient(
            RestClient.Builder restClientBuilder,
            @Value("${validation-service.base-url}") String baseUrl) {
        this.restClient = restClientBuilder.baseUrl(baseUrl).build();
    }

    public boolean validate(String customerId, String productId) {

        try {
            OrderValidationResponse response =
                    restClient
                            .get()
                            .uri(
                                    "/validation/customers/{customerId}/products/{productId}",
                                    customerId,
                                    productId)
                            .retrieve()
                            .body(OrderValidationResponse.class);

            return response != null && response.valid();
        } catch (HttpClientErrorException e) {
            return false;
        }
    }
}
