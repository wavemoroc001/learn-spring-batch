package org.example.batchkafka.batch.worker;

import org.example.batchkafka.customer.Customer;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class CustomerWriter implements ItemWriter<Customer> {
    @Override
    public void write(Chunk<? extends Customer> chunk) throws Exception {

    }
}
