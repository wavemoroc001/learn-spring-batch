package org.example.batchkafka.batch.worker;

import lombok.extern.slf4j.Slf4j;
import org.example.batchkafka.customer.Customer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@Slf4j
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        customer.setSalary(customer.getSalary().add(new BigDecimal(100)));
        log.info("Processing customer information: {}", customer);
        return customer;
    }
}
