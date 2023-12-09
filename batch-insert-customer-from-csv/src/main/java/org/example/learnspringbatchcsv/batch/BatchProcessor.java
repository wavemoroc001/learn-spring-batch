package org.example.learnspringbatchcsv.batch;

import lombok.extern.slf4j.Slf4j;
import org.example.learnspringbatchcsv.customer.Customer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BatchProcessor implements ItemProcessor<Customer, Customer> {

    // from item reader will read csv file and send customer object to processor
    // in this case no need to do any processing so just return customer object
    @Override
    public Customer process(Customer customer) throws Exception {
        log.info("Processing customer: {}", customer);
        return customer;
    }
}
