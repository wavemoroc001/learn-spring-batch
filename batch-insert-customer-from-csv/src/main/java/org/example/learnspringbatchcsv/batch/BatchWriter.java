package org.example.learnspringbatchcsv.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.learnspringbatchcsv.customer.Customer;
import org.example.learnspringbatchcsv.customer.CustomerRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchWriter implements ItemWriter<Customer> {

    private final CustomerRepository customerRepository;

    @Override
    public void write(Chunk<? extends Customer> chunk) throws Exception {
        chunk.getItems().forEach(customer -> log.info("Writing customer: {}", customer));
        customerRepository.saveAll(chunk.getItems());
        log.info("save customer: {} to db", chunk.getItems().size());
    }
}
